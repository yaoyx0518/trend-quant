"""Unit tests for backtest.benchmark — benchmark simulations."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backtest.benchmark import equal_weight_pool_benchmark, single_symbol_benchmark


def _make_bench_df(prices: list[float], dates: list[str]) -> pd.DataFrame:
    records = []
    for d, close in zip(dates, prices):
        records.append({
            "date": date.fromisoformat(d),
            "open": close * 0.99,
            "high": close * 1.01,
            "low": close * 0.98,
            "close": close,
            "volume": 1_000_000,
        })
    return pd.DataFrame(records)


class TestEqualWeightPoolBenchmark:
    def test_single_symbol(self) -> None:
        df = _make_bench_df([10.0, 11.0, 12.0], ["2025-01-06", "2025-01-07", "2025-01-08"])
        timeline = [date(2025, 1, 6), date(2025, 1, 7), date(2025, 1, 8)]
        result = equal_weight_pool_benchmark({"TEST": df}, timeline, 100_000)
        assert result["name"] == "equal_weight_pool"
        assert len(result["series"]) == 3
        # Equity should be > 0 after buying
        assert result["series"][-1]["equity"] > 0

    def test_empty_timeline(self) -> None:
        result = equal_weight_pool_benchmark({}, [], 100_000)
        assert result["series"] == []

    def test_no_symbols(self) -> None:
        result = equal_weight_pool_benchmark({}, [date(2025, 1, 6)], 100_000)
        assert result["series"] == []


class TestSingleSymbolBenchmark:
    def test_basic(self) -> None:
        df = _make_bench_df([10.0, 11.0], ["2025-01-06", "2025-01-07"])
        timeline = [date(2025, 1, 6), date(2025, 1, 7)]
        result = single_symbol_benchmark(df, timeline, 100_000, symbol="TEST")
        assert "symbol:TEST" in result["name"]
        assert len(result["series"]) == 2

    def test_empty_benchmark_data(self) -> None:
        result = single_symbol_benchmark(pd.DataFrame(), [date(2025, 1, 6)], 100_000)
        assert result["series"] == []
