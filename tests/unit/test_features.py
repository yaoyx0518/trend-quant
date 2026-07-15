"""Unit tests for strategy.features — feature builders."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from strategy.features import build_trend_score_features, build_momentum_features, _momentum, _ma


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bars(n: int = 50) -> pd.DataFrame:
    """Generate clean bullish bars for feature testing."""
    rng = np.random.default_rng(42)
    price = 10.0
    records = []
    for i in range(n):
        change = 0.01 + rng.normal(0, 0.005)
        close = price * (1 + change)
        records.append({
            "open": price,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": 1_000_000 + i * 10_000,
        })
        price = close
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# _momentum
# ---------------------------------------------------------------------------

class TestMomentum:
    def test_positive_momentum(self) -> None:
        s = pd.Series([10.0, 10.5, 11.0, 11.5, 12.0])
        result = _momentum(s, window=4)
        assert result == pytest.approx(0.2, abs=0.01)  # 12/10 - 1

    def test_negative_momentum(self) -> None:
        s = pd.Series([12.0, 11.5, 11.0, 10.5, 10.0])
        result = _momentum(s, window=4)
        assert result == pytest.approx(-0.1666, abs=0.01)

    def test_insufficient_data(self) -> None:
        s = pd.Series([10.0, 11.0, 12.0])
        assert _momentum(s, window=5) is None

    def test_empty_series(self) -> None:
        assert _momentum(pd.Series(dtype=float), window=5) is None

    def test_zero_window(self) -> None:
        s = pd.Series([10.0, 11.0])
        assert _momentum(s, window=0) is None

    def test_zero_price_returns_none(self) -> None:
        s = pd.Series([0.0, 11.0, 12.0])
        assert _momentum(s, window=2) is None


# ---------------------------------------------------------------------------
# _ma
# ---------------------------------------------------------------------------

class TestMA:
    def test_simple_moving_average(self) -> None:
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = _ma(s, window=3)
        assert result == pytest.approx(4.0)  # mean of [3,4,5]

    def test_insufficient_window(self) -> None:
        s = pd.Series([1.0, 2.0])
        assert _ma(s, window=5) is None


# ---------------------------------------------------------------------------
# build_trend_score_features
# ---------------------------------------------------------------------------

class TestBuildTrendScoreFeatures:
    def test_ok_signal_structure(self, default_cfg: dict) -> None:
        bars = _make_bars(50)
        state = {}
        result = build_trend_score_features(bars, state, default_cfg)
        assert result["ok"] is True
        assert isinstance(result["trend_score"], float)
        assert isinstance(result["price_direction"], float)
        assert isinstance(result["confidence"], float)
        assert isinstance(result["calc_details"], dict)
        assert "prev_prev_score" in result["calc_details"]
        assert "position_qty" in result["calc_details"]

    def test_passes_state_values(self, default_cfg: dict) -> None:
        bars = _make_bars(50)
        state = {
            "prev_prev_trend_score": 20.0,
            "prev_trend_score": 25.0,
            "position_qty": 100,
            "sellable_qty": 50,
            "hard_stop_price": 9.5,
            "chandelier_stop_price": 9.8,
        }
        result = build_trend_score_features(bars, state, default_cfg)
        details = result["calc_details"]
        assert details["prev_prev_score"] == 20.0
        assert details["prev_score"] == 25.0
        assert details["position_qty"] == 100
        assert details["sellable_qty"] == 50
        assert details["hard_stop_price"] == 9.5
        assert details["chandelier_stop_price"] == 9.8

    def test_insufficient_bars(self, default_cfg: dict) -> None:
        bars = _make_bars(5)  # too few
        result = build_trend_score_features(bars, {}, default_cfg)
        assert result["ok"] is False
        assert result["reason"] == "insufficient_bars"


# ---------------------------------------------------------------------------
# build_momentum_features
# ---------------------------------------------------------------------------

class TestBuildMomentumFeatures:
    def test_ok_signal_structure(self, default_cfg: dict) -> None:
        bars = _make_bars(80)  # need enough bars for ma200
        cfg = {**default_cfg, "momentum_window_short": 10, "momentum_window_long": 20}
        result = build_momentum_features(bars, {}, cfg)
        assert result["ok"] is True
        assert "momentum_short" in result
        assert "momentum_long" in result
        assert "momentum_mix" in result
        assert "calc_details" in result

    def test_momentum_mix_weighted(self, default_cfg: dict) -> None:
        bars = _make_bars(60)
        cfg = {
            **default_cfg,
            "momentum_window_short": 5,
            "momentum_window_long": 20,
            "momentum_weight_short": 0.8,
            "momentum_weight_long": 0.2,
        }
        result = build_momentum_features(bars, {}, cfg)
        if result["momentum_short"] is not None and result["momentum_long"] is not None:
            expected_mix = 0.8 * result["momentum_short"] + 0.2 * result["momentum_long"]
            assert result["momentum_mix"] == pytest.approx(expected_mix)

    def test_contains_ma_values(self, default_cfg: dict) -> None:
        bars = _make_bars(200)
        result = build_momentum_features(bars, {}, default_cfg)
        details = result["calc_details"]
        for key in ("ma20", "ma30", "ma40", "ma60", "ma200"):
            assert details[key] is not None, f"{key} should not be None"
            assert details[key] > 0, f"{key} should be positive"

    def test_passes_state_values(self, default_cfg: dict) -> None:
        bars = _make_bars(80)
        state = {
            "hard_stop_price": 7.0,
            "chandelier_stop_price": 6.5,
            "position_qty": 200,
            "sellable_qty": 200,
            "prev_trend_score": 30.0,
        }
        result = build_momentum_features(bars, state, default_cfg)
        details = result["calc_details"]
        assert details["hard_stop_price"] == 7.0
        assert details["position_qty"] == 200
        assert details["prev_score"] == 30.0
