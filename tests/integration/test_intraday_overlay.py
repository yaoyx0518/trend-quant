"""P1.4 tests — intraday row appended to cached EOD series must equal the
last row of a full-history recompute including the synthetic bar."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core import indicators as core_ind
from core.indicators import INDICATOR_FORMULA_VERSION
from data.indicator_store import (
    compute_indicator_frame,
    compute_intraday_row,
    get_series_with_intraday,
)
from data.storage.db import Database


def _make_bars(seed: int = 3, n: int = 260) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    closes = 100 + np.cumsum(rng.normal(0.1, 1.2, n))
    dates = pd.date_range("2025-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {
            "time": dates,
            "open": closes,
            "high": closes + np.abs(rng.normal(0, 0.4, n)),
            "low": closes - np.abs(rng.normal(0, 0.4, n)),
            "close": closes,
            "volume": np.abs(rng.normal(1e6, 2e5, n)),
        }
    )


@pytest.fixture
def setup(tmp_path):
    db = Database(tmp_path / "test.db")
    bars = _make_bars()
    db.save_market_data("T.SS", bars, price_mode="qfq")
    db.save_indicator_daily("T.SS", compute_indicator_frame(bars), INDICATOR_FORMULA_VERSION)

    # Synthetic intraday bar (not-yet-closed day, absent from stored history).
    prev_close = float(bars["close"].iloc[-1])
    synth_time = pd.Timestamp(bars["time"].iloc[-1]) + pd.Timedelta(days=1, hours=10)
    synth = {
        "time": synth_time,
        "open": prev_close * 1.001,
        "high": prev_close * 1.01,
        "low": prev_close * 0.995,
        "close": prev_close * 1.005,
        "volume": 3e5,
    }

    # Reference: full-history recompute WITH the synthetic bar appended.
    synth_row = pd.DataFrame([{**synth, "time": synth["time"]}])
    full = pd.concat([bars, synth_row], ignore_index=True)
    return db, bars, synth, full, synth_time


INDICATORS = [
    "sma5", "sma20", "sma60", "sma200",
    "ema5", "ema20", "boll_mid", "boll_up", "boll_dn",
    "atr", "vol_ma20", "er10",
    "macd_dif", "macd_dea", "macd_hist", "rsi14",
]


class TestIntradayRow:
    def test_row_matches_full_recompute(self, setup) -> None:
        db, bars, synth, full, synth_time = setup
        row = compute_intraday_row("T.SS", synth, db=db)
        close = pd.to_numeric(full["close"], errors="coerce")
        volume = pd.to_numeric(full["volume"], errors="coerce")

        expected: dict[str, float] = {}
        for n in (5, 20, 60, 200):
            expected[f"sma{n}"] = core_ind.sma(close, n).iloc[-1]
        for n in (5, 20):
            expected[f"ema{n}"] = core_ind.ema(close, n).iloc[-1]
        boll = core_ind.bollinger(close)
        expected.update({"boll_mid": boll["mid"].iloc[-1], "boll_up": boll["up"].iloc[-1], "boll_dn": boll["dn"].iloc[-1]})
        expected["atr"] = core_ind.atr(full, 20).iloc[-1]
        expected["vol_ma20"] = core_ind.sma(volume, 20).iloc[-1]
        expected["er10"] = core_ind.efficiency_ratio(close, 10).iloc[-1]
        macd_out = core_ind.macd(close, warmup=True)
        expected["macd_dif"] = macd_out["dif"].iloc[-1]
        expected["macd_dea"] = macd_out["dea"].iloc[-1]
        expected["macd_hist"] = macd_out["hist"].iloc[-1]
        expected["rsi14"] = core_ind.rsi(close, 14).iloc[-1]

        for name in INDICATORS:
            assert name in row, f"{name} missing from intraday row"
            assert row[name] == pytest.approx(expected[name], rel=1e-9, abs=1e-9), (
                f"{name}: overlay={row[name]} vs full={expected[name]}"
            )

    def test_appended_series_has_intraday_row(self, setup) -> None:
        db, bars, synth, full, synth_time = setup
        row = compute_intraday_row("T.SS", synth, db=db)
        series = get_series_with_intraday("T.SS", "sma20", intraday_row=row, db=db)
        assert series.index[-1] == synth_time
        assert series.iloc[-1] == pytest.approx(row["sma20"])

    def test_series_without_row_is_eod_only(self, setup) -> None:
        db, bars, synth, full, synth_time = setup
        series = get_series_with_intraday("T.SS", "sma20", intraday_row=None, db=db)
        assert series.index[-1] < synth_time
