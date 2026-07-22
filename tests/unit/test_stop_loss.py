"""Unit tests for services.stop_loss (止损价计算的单一实现来源).

覆盖：
- 硬止损以「买入价 − 1.5×ATR(买入日)」计算，而非买入当日收盘价
- 吊灯止损公式与 per-instrument stop_atr_mul 覆盖
- 边界：无数据、非交易日、无效输入
"""

from __future__ import annotations

import pandas as pd
import pytest

from core.strategy_config import DEFAULT_STRATEGY_CONFIG
from data.indicator_store import compute_live_series
from services import stop_loss as sl


@pytest.fixture(autouse=True)
def _default_strategy_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin strategy config to code defaults (global DB may be uninitialized)."""
    monkeypatch.setattr(sl, "get_strategy_config", lambda: dict(DEFAULT_STRATEGY_CONFIG))
    # 默认走纯 EOD 路径，避免测试在交易时段访问实时行情；
    # 盘中行为由 TestComputeStopLossIntraday 显式注入合成K线验证。
    monkeypatch.setattr(sl, "_fetch_intraday_bar", lambda symbol, df: None)


@pytest.fixture
def bull_db(test_db):
    from conftest import make_bull_bars

    bars = make_bull_bars(40)
    test_db.save_market_data("510300.SS", bars, price_mode="qfq")
    return test_db, bars


def _buy_inputs(bars: pd.DataFrame, idx: int = -3) -> tuple[str, float]:
    row = bars.iloc[idx]
    buy_date = str(row["time"])[:10]
    # 买入价刻意偏离当日收盘价 —— 手工买入通常不是收盘价成交
    buy_price = round(float(row["close"]) * 0.97, 4)
    return buy_date, buy_price


class TestComputeStopLoss:
    def test_hard_stop_uses_buy_price_not_close(self, bull_db) -> None:
        db, bars = bull_db
        buy_date, buy_price = _buy_inputs(bars)

        out = sl.compute_stop_loss("510300", buy_date, buy_price, db=db)

        atr_series = compute_live_series(bars, "atr")
        atr_at_buy = float(atr_series[atr_series.index <= pd.Timestamp(buy_date)].iloc[-1])
        expected = round(buy_price - 1.5 * atr_at_buy, 4)
        assert out["hard_stop_price"] == pytest.approx(expected)
        assert out["hard_stop_atr_mul"] == 1.5
        assert out["atr_at_buy"] == pytest.approx(round(atr_at_buy, 4))
        # 若以收盘价为基准，结果会不同 —— 确保没有回退到收盘价口径
        close_based = round(float(bars.iloc[-3]["close"]) - 1.5 * atr_at_buy, 4)
        assert out["hard_stop_price"] != pytest.approx(close_based)

    def test_chandelier_stop(self, bull_db) -> None:
        db, bars = bull_db
        buy_date, buy_price = _buy_inputs(bars)

        out = sl.compute_stop_loss("510300.SS", buy_date, buy_price, db=db)

        atr_series = compute_live_series(bars, "atr")
        current_atr = float(atr_series.iloc[-1])
        buy_ts = pd.Timestamp(buy_date)
        highest = float(bars[pd.to_datetime(bars["time"]) >= buy_ts]["high"].max())
        expected = round(highest - 2.5 * current_atr, 4)
        assert out["chandelier_stop_price"] == pytest.approx(expected)
        assert out["chandelier_stop_atr_mul"] == 2.5
        assert out["highest_since_buy"] == pytest.approx(round(highest, 4))

    def test_per_instrument_stop_atr_mul_override(self, bull_db) -> None:
        db, bars = bull_db
        db.save_instrument_metadata([{"symbol": "510300.SS", "name": "沪深300ETF", "stop_atr_mul": 2.0}])
        buy_date, buy_price = _buy_inputs(bars)

        out = sl.compute_stop_loss("510300.SS", buy_date, buy_price, db=db)

        atr_series = compute_live_series(bars, "atr")
        atr_at_buy = float(atr_series[atr_series.index <= pd.Timestamp(buy_date)].iloc[-1])
        assert out["hard_stop_atr_mul"] == 2.0
        assert out["hard_stop_price"] == pytest.approx(round(buy_price - 2.0 * atr_at_buy, 4))

    def test_non_trading_day_buy_date_uses_lookback_atr(self, bull_db) -> None:
        db, bars = bull_db
        # 取一个交易日，顺延到周日（非交易日）买入
        row = bars.iloc[-4]
        friday_or_later = pd.Timestamp(str(row["time"])[:10])
        sunday = friday_or_later + pd.Timedelta(days=(6 - friday_or_later.weekday()) % 7 or 7)
        buy_date = str(sunday.date())

        out = sl.compute_stop_loss("510300.SS", buy_date, 1.0, db=db)

        atr_series = compute_live_series(bars, "atr")
        atr_at_buy = float(atr_series[atr_series.index <= sunday].iloc[-1])
        assert out["hard_stop_price"] == pytest.approx(round(1.0 - 1.5 * atr_at_buy, 4))

    def test_no_data_raises(self, test_db) -> None:
        with pytest.raises(sl.StopLossError, match="未找到"):
            sl.compute_stop_loss("999999.SS", "2025-01-10", 1.0, db=test_db)

    def test_invalid_symbol_raises(self, test_db) -> None:
        with pytest.raises(sl.StopLossError, match="无效"):
            sl.compute_stop_loss("   ", "2025-01-10", 1.0, db=test_db)

    def test_invalid_price_raises(self, bull_db) -> None:
        db, bars = bull_db
        buy_date, _ = _buy_inputs(bars)
        with pytest.raises(sl.StopLossError, match="大于 0"):
            sl.compute_stop_loss("510300.SS", buy_date, 0.0, db=db)


class TestComputeStopLossIntraday:
    """盘中实时叠加：当日合成K线计入最高价/最新价，ATR 保持历史完整K线口径。"""

    @staticmethod
    def _synth_bar(high: float, close: float, low: float | None = None) -> dict:
        return {
            "time": pd.Timestamp("2025-03-03 10:30:00"),  # 晚于 bull bars 末日
            "open": close,
            "high": high,
            "low": low if low is not None else close * 0.99,
            "close": close,
            "volume": 0.0,
            "amount": 0.0,
        }

    def test_intraday_bar_updates_high_and_latest(self, bull_db, monkeypatch) -> None:
        db, bars = bull_db
        buy_date, buy_price = _buy_inputs(bars)
        eod = sl.compute_stop_loss("510300.SS", buy_date, buy_price, db=db)
        assert eod["is_intraday"] is False

        synth_high = eod["highest_since_buy"] * 1.05
        synth_close = eod["highest_since_buy"] * 1.04
        monkeypatch.setattr(
            sl, "_fetch_intraday_bar",
            lambda symbol, df: self._synth_bar(synth_high, synth_close),
        )

        out = sl.compute_stop_loss("510300.SS", buy_date, buy_price, db=db)

        assert out["is_intraday"] is True
        assert out["intraday_bar"]["date"] == "2025-03-03"
        assert out["highest_since_buy"] == pytest.approx(round(synth_high, 4))
        assert out["latest_price"] == pytest.approx(round(synth_close, 4))
        # ATR 不被当日不完整K线污染 → 硬止损不变
        assert out["current_atr"] == eod["current_atr"]
        assert out["hard_stop_price"] == eod["hard_stop_price"]
        # 吊灯止损随盘中新高实时上移
        current_atr = float(compute_live_series(bars, "atr").iloc[-1])
        expected = round(synth_high - 2.5 * current_atr, 4)
        assert out["chandelier_stop_price"] == pytest.approx(expected)
        assert out["chandelier_stop_price"] > eod["chandelier_stop_price"]

    def test_intraday_unavailable_falls_back_to_eod(self, bull_db, monkeypatch) -> None:
        db, bars = bull_db
        buy_date, buy_price = _buy_inputs(bars)
        eod = sl.compute_stop_loss("510300.SS", buy_date, buy_price, db=db)

        monkeypatch.setattr(sl, "_fetch_intraday_bar", lambda symbol, df: None)
        out = sl.compute_stop_loss("510300.SS", buy_date, buy_price, db=db)

        assert out["is_intraday"] is False
        assert "intraday_bar" not in out
        assert out["chandelier_stop_price"] == eod["chandelier_stop_price"]
