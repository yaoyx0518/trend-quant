"""Unit tests for strategy.indicators — atr() and efficiency_ratio()."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.indicators import atr, efficiency_ratio


class TestATR:
    def test_standard_input(self) -> None:
        df = pd.DataFrame(
            {
                "high": [11.0, 12.0, 13.0, 14.0, 15.0],
                "low": [9.0, 10.0, 11.0, 12.0, 13.0],
                "close": [10.0, 11.0, 12.0, 13.0, 14.0],
            }
        )
        result = atr(df, period=3)
        assert len(result) == 5
        # ATR must be positive for non‑zero ranges
        assert result.iloc[-1] > 0
        assert result.iloc[0] > 0  # first bar uses min_periods=1

    def test_period_one(self) -> None:
        """period=1: each ATR equals the true range of that bar."""
        df = pd.DataFrame(
            {
                "high": [12.0, 13.0],
                "low": [8.0, 9.0],
                "close": [10.0, 11.0],
            }
        )
        result = atr(df, period=1)
        # Bar 0: TR = max(12-8=4, |12-?|, |8-?|) — close.shift(1) is NaN
        # TR = max(4, NaN, NaN) = 4
        assert result.iloc[0] == pytest.approx(4.0, abs=0.01)

    def test_all_identical_prices(self) -> None:
        """When high=low=close, True Range is zero, ATR is zero."""
        df = pd.DataFrame(
            {
                "high": [10.0] * 10,
                "low": [10.0] * 10,
                "close": [10.0] * 10,
            }
        )
        result = atr(df, period=5)
        assert result.iloc[-1] == 0.0

    def test_empty_dataframe(self) -> None:
        result = atr(pd.DataFrame(), period=20)
        assert len(result) == 0

    def test_single_row(self) -> None:
        df = pd.DataFrame({"high": [11.0], "low": [9.0], "close": [10.0]})
        result = atr(df, period=20)
        assert len(result) == 1
        assert result.iloc[0] == 2.0  # TR = high‑low = 2


class TestEfficiencyRatio:
    def test_strong_trend(self) -> None:
        """A straight line has ER ≈ 1.0."""
        s = pd.Series(np.linspace(10, 20, 30), dtype=float)
        result = efficiency_ratio(s, period=10)
        assert result.iloc[-1] == pytest.approx(1.0, abs=0.001)

    def test_noisy_sideways(self) -> None:
        """White noise has low ER."""
        rng = np.random.default_rng(123)
        s = pd.Series(10.0 + rng.normal(0, 0.5, 50), dtype=float)
        result = efficiency_ratio(s, period=10)
        assert result.iloc[-1] < 0.5

    def test_period_larger_than_data(self) -> None:
        s = pd.Series([10.0, 11.0, 12.0], dtype=float)
        result = efficiency_ratio(s, period=10)
        # change = |12-?|=NaN, volatility = ..., ER = 0 after fillna
        assert result.iloc[-1] == 0.0

    def test_empty_series(self) -> None:
        result = efficiency_ratio(pd.Series(dtype=float), period=10)
        assert len(result) == 0

    def test_constant_series(self) -> None:
        """A flat line: change=0, volatility=0 → ER=0 (0/0 → NaN → 0)."""
        s = pd.Series([5.0] * 30, dtype=float)
        result = efficiency_ratio(s, period=10)
        assert result.iloc[-1] == 0.0
