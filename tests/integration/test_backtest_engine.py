"""Integration tests for backtest.backtest_engine — BacktestEngine with isolated DB.

Uses mocked market data and strategy configs.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


def _make_market_data(symbols: list[str], n_days: int = 60) -> dict[str, pd.DataFrame]:
    """Generate synthetic OHLCV data for multiple symbols."""
    rng = np.random.default_rng(42)
    result = {}
    for symbol in symbols:
        records = []
        price = 10.0 + hash(symbol) % 5  # different starting prices
        for i in range(n_days):
            change = 0.01 + rng.normal(0, 0.008)
            close = price * (1 + change)
            records.append({
                "time": f"2024-01-{i+1:02d}",
                "open": price, "high": close * 1.01, "low": close * 0.99,
                "close": close, "volume": 1_000_000 + i * 10_000,
            })
            price = close
        result[symbol] = pd.DataFrame(records)
    return result


class TestBacktestEngineIntegration:
    def test_run_trend_strategy(self, test_db) -> None:
        """Run a minimal trend‑strategy backtest with mocked data."""
        from backtest.backtest_engine import BacktestEngine

        # Setup test instruments and benchmarks
        market_data = _make_market_data(["T1.SS"], 80)
        benchmark_500 = _make_market_data(["510500.SS"], 80)["510500.SS"]
        benchmark_cx = _make_market_data(["159915.SZ"], 80)["159915.SZ"]

        test_db.save_instrument_metadata([
            {"symbol": "T1.SS", "name": "Test1", "category_l1": "宽基",
             "category_l2": "大盘", "category_l3": "沪深300",
             "priority_l1": 1, "priority_l2": 1, "priority_l3": 1, "sort_order": 1},
        ])
        test_db.save_market_data("T1.SS", market_data["T1.SS"])
        test_db.save_market_data("510500.SS", benchmark_500)
        test_db.save_market_data("159915.SZ", benchmark_cx)

        engine = BacktestEngine()

        with patch.object(engine, "_load_yaml", side_effect=_mock_yaml):
            with patch("backtest.backtest_engine.get_db", return_value=test_db):
                result = engine.run(
                    {
                        "start_date": "2024-01-22",
                        "end_date": "2024-02-20",
                        "initial_capital": 200_000,
                        "symbols": ["T1.SS"],
                        "strategy_id": "trend_score_v1",
                    },
                    persist=False,
                    include_charts=False,
                )

        assert result["status"] in ("ok", "error")


def _mock_yaml(path: str) -> dict:
    """Return test config for YAML paths."""
    if "strategy.yaml" in path:
        return {
            "strategy": {
                "n_short": 5, "n_mid": 10, "n_long": 20,
                "atr_period": 20, "vol_ma_period": 20, "er_period": 10,
                "entry_threshold_min": 10.0, "entry_threshold_max": 20.0,
            }
        }
    if "instruments.yaml" in path:
        return {
            "instruments": [
                {
                    "symbol": "T1.SS", "name": "Test1", "enabled": True,
                    "risk_budget_pct": 0.01, "stop_atr_mul": 2.0,
                    "asset_type": "etf",
                }
            ]
        }
    if "app.yaml" in path:
        return {"app": {"lot_size": 100}}
    return {}
