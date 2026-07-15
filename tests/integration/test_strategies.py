"""Integration tests for MarketStore, signal engine, strategies, and portfolio."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from data.storage.market_store import MarketStore
from strategy.trend_score_strategy import TrendScoreStrategy
from strategy.momentum_topn_strategy import MomentumTopNStrategy
from strategy.momentum_topn_v2_strategy import MomentumTopNStrategyV2
from portfolio.risk_sizer import RiskSizer
from portfolio.service import PortfolioService


# ---------------------------------------------------------------------------
# MarketStore
# ---------------------------------------------------------------------------

class TestMarketStore:
    def test_save_and_load(self, test_db) -> None:
        store = MarketStore(db=test_db, price_mode="qfq")
        df = pd.DataFrame([{
            "time": "2025-01-06", "open": 10.0, "high": 11.0, "low": 9.0,
            "close": 10.5, "volume": 1_000_000, "amount": 1_050_000, "provider": "test",
        }])
        store.save_history("A.SS", df)
        loaded = store.load_history("A.SS")
        assert len(loaded) == 1
        assert float(loaded.iloc[0]["close"]) == 10.5

    def test_list_stored_symbols(self, test_db) -> None:
        store = MarketStore(db=test_db, price_mode="qfq")
        df = pd.DataFrame([{
            "time": "2025-01-06", "open": 10.0, "high": 11.0, "low": 9.0,
            "close": 10.5, "volume": 1_000_000, "amount": 1_050_000, "provider": "test",
        }])
        store.save_history("X.SS", df)
        store.save_history("Y.SZ", df)
        symbols = store.list_stored_symbols()
        assert "X.SS" in symbols
        assert "Y.SZ" in symbols

    def test_separate_price_modes(self, test_db) -> None:
        qfq = MarketStore(db=test_db, price_mode="qfq")
        raw = MarketStore(db=test_db, price_mode="raw")
        df_q = pd.DataFrame([{
            "time": "2025-01-06", "open": 1.0, "high": 2.0, "low": 0.5,
            "close": 1.5, "volume": 100, "amount": 150, "provider": "test",
        }])
        df_r = pd.DataFrame([{
            "time": "2025-01-06", "open": 2.0, "high": 3.0, "low": 1.5,
            "close": 2.5, "volume": 100, "amount": 250, "provider": "test",
        }])
        qfq.save_history("Z.SS", df_q)
        raw.save_history("Z.SS", df_r)
        assert float(qfq.load_history("Z.SS").iloc[0]["close"]) == 1.5
        assert float(raw.load_history("Z.SS").iloc[0]["close"]) == 2.5


# ---------------------------------------------------------------------------
# TrendScoreStrategy
# ---------------------------------------------------------------------------

def _make_bars(n: int = 50) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    price = 10.0
    records = []
    for i in range(n):
        change = 0.01 + rng.normal(0, 0.005)
        close = price * (1 + change)
        records.append({
            "open": price, "high": close * 1.01, "low": close * 0.99,
            "close": close, "volume": 1_000_000 + i * 10_000,
        })
        price = close
    return pd.DataFrame(records)


class TestTrendScoreStrategy:
    def test_complete_evaluate_returns_signal(self, default_cfg) -> None:
        strategy = TrendScoreStrategy()
        bars = _make_bars(60)
        state = {"position_qty": 0}
        signal = strategy.evaluate("TEST", bars, state, default_cfg)
        assert "ok" in signal
        assert "trend_score" in signal
        assert "action" in signal
        assert "calc_details" in signal

    def test_required_history_bars(self, default_cfg) -> None:
        strategy = TrendScoreStrategy()
        bars = strategy.required_history_bars(default_cfg)
        assert bars >= 20

    def test_with_position_state(self, default_cfg) -> None:
        strategy = TrendScoreStrategy()
        bars = _make_bars(60)
        state = {
            "position_qty": 100,
            "sellable_qty": 100,
            "hard_stop_price": 9.0,
            "chandelier_stop_price": 9.5,
        }
        signal = strategy.evaluate("TEST", bars, state, default_cfg)
        assert signal["ok"] is True


class TestMomentumStrategies:
    def _momentum_cfg(self) -> dict:
        return {
            "n_short": 5, "n_mid": 10, "n_long": 20, "atr_period": 20,
            "momentum_window_short": 10, "momentum_window_long": 20,
            "momentum_weight_short": 0.6, "momentum_weight_long": 0.4,
            "hybrid_weight_momentum": 1.0, "hybrid_weight_trend": 0.0,
            "max_holdings": 5, "rebalance_frequency": "weekly",
            "rebalance_weekday": 1,
        }

    def test_momentum_v1_evaluate(self) -> None:
        strategy = MomentumTopNStrategy()
        bars = _make_bars(200)
        cfg = self._momentum_cfg()
        signal = strategy.evaluate("TEST", bars, {}, cfg)
        assert signal["ok"] is True
        assert signal["action"] is not None

    def test_momentum_v2_evaluate_with_exit_rules(self) -> None:
        strategy = MomentumTopNStrategyV2()
        bars = _make_bars(200)
        cfg = self._momentum_cfg()
        signal = strategy.evaluate("TEST", bars, {}, cfg)
        assert signal["ok"] is True


# ---------------------------------------------------------------------------
# RiskSizer
# ---------------------------------------------------------------------------

class TestRiskSizerIntegration:
    def test_full_sizing_pipeline(self) -> None:
        sizer = RiskSizer(lot_size=100)
        qty = sizer.suggest_qty(200_000, 0.01, 0.5, 2.0)
        assert qty == 2000
        capped = sizer.cap_qty_by_max_cost(qty, 10.0, 15_000)
        assert capped < qty  # 2000*10=20000 > 15000
        assert capped % 100 == 0
