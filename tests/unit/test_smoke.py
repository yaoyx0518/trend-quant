"""Smoke test — verify the test infrastructure works end‑to‑end.

Tests the core fixtures and confirms pytest + markers + coverage are wired.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from core.trend import calculate_trend_score_snapshot, safe_float
from core.indicators import atr, efficiency_ratio


class TestSafeFloat:
    def test_none_returns_default(self):
        assert safe_float(None) == 0.0
        assert safe_float(None, default=-1.0) == -1.0

    def test_nan_returns_default(self):
        assert safe_float(float("nan")) == 0.0

    def test_normal_number(self):
        assert safe_float(42.5) == 42.5
        assert safe_float(0) == 0.0
        assert safe_float(-3.14) == -3.14

    def test_string_coercion(self):
        assert safe_float("12.34") == 12.34

    def test_bool(self):
        assert safe_float(True) == 1.0
        assert safe_float(False) == 0.0


class TestATR:
    def test_standard_input(self):
        df = pd.DataFrame(
            {
                "high": [11.0, 12.0, 13.0, 14.0, 15.0],
                "low": [9.0, 10.0, 11.0, 12.0, 13.0],
                "close": [10.0, 11.0, 12.0, 13.0, 14.0],
            }
        )
        result = atr(df, period=3)
        assert len(result) == 5
        assert result.iloc[-1] > 0

    def test_empty_dataframe(self):
        result = atr(pd.DataFrame(), period=20)
        assert len(result) == 0


class TestTrendScoreSnapshot:
    def test_bullish_scores_positive(self, default_cfg):
        """Bullish bars should produce a positive trend score."""
        from conftest import make_bull_bars

        bars = make_bull_bars(40)
        result = calculate_trend_score_snapshot(bars, default_cfg)
        assert result["ok"] is True
        assert isinstance(result["trend_score"], float)
        # In a steady uptrend the score should be positive
        assert result["trend_score"] > 0, f"Expected positive trend_score, got {result['trend_score']}"

    def test_bearish_scores_negative(self, default_cfg):
        """Bearish bars should produce a negative trend score."""
        from conftest import make_bear_bars

        bars = make_bear_bars(40)
        result = calculate_trend_score_snapshot(bars, default_cfg)
        assert result["ok"] is True
        assert result["trend_score"] < 0, f"Expected negative trend_score, got {result['trend_score']}"

    def test_insufficient_bars(self, default_cfg):
        """Too few bars should yield ok=False."""
        from conftest import make_short_bars

        bars = make_short_bars(5)
        result = calculate_trend_score_snapshot(bars, default_cfg)
        assert result["ok"] is False
        assert result["reason"] == "insufficient_bars"

    def test_flat_bars_atr_zero(self, default_cfg):
        """Constant‑price bars should produce invalid_atr."""
        from conftest import make_flat_bars

        bars = make_flat_bars(30)
        result = calculate_trend_score_snapshot(bars, default_cfg)
        assert result["ok"] is False
        assert result["reason"] == "invalid_atr"

    def test_fixed_atr_overrides_computed(self, default_cfg):
        """When fixed_atr is provided, it should be used instead of computed ATR."""
        from conftest import make_bull_bars

        bars = make_bull_bars(40)
        result_fixed = calculate_trend_score_snapshot(bars, default_cfg, fixed_atr=1.5)
        result_computed = calculate_trend_score_snapshot(bars, default_cfg)

        assert result_fixed["ok"] is True
        assert result_fixed["atr"] == 1.5
        assert result_fixed["atr"] != result_computed["atr"]

    def test_fixed_volume_overrides_current(self, default_cfg):
        """fixed_volume should replace the last bar's volume for vol_ratio."""
        from conftest import make_bull_bars

        bars = make_bull_bars(40)
        result = calculate_trend_score_snapshot(bars, default_cfg, fixed_volume=10_000_000)
        assert result["ok"] is True
        details = result["calc_details"]
        assert details["current_volume"] == 10_000_000

    def test_trend_score_clamped_to_100(self, default_cfg):
        """Trend score must be within [-100, 100]."""
        from conftest import make_bull_bars

        bars = make_bull_bars(40)
        result = calculate_trend_score_snapshot(bars, default_cfg)
        assert -100.0 <= result["trend_score"] <= 100.0
        assert 0.0 <= result["confidence"] <= 1.0
