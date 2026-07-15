"""Unit tests for strategy.momentum_signal_modules — signal handlers and registry."""

from __future__ import annotations

import pytest

from strategy.momentum_signal_modules import (
    normalize_signal_modules,
    buy_filter_price_above_ma20,
    buy_filter_price_above_ma60,
    buy_filter_price_above_ma200,
    buy_filter_trend_score_max,
    sell_signal_hard_stop,
    sell_signal_chandelier_stop,
    sell_signal_ma_breakdown_max,
    BUY_FILTER_REGISTRY,
    SELL_SIGNAL_REGISTRY,
    DEFAULT_MOMENTUM_BUY_FILTERS,
    DEFAULT_MOMENTUM_SELL_SIGNALS,
)


# ---------------------------------------------------------------------------
# normalize_signal_modules
# ---------------------------------------------------------------------------

class TestNormalizeSignalModules:
    def test_list_input(self) -> None:
        assert normalize_signal_modules(["a", "b"], ["x"]) == ["a", "b"]

    def test_comma_separated_string(self) -> None:
        assert normalize_signal_modules("a, b , c", ["x"]) == ["a", "b", "c"]

    def test_pipe_separated_string(self) -> None:
        assert normalize_signal_modules("a|b|c", ["x"]) == ["a", "b", "c"]

    def test_semicolon_separated_string(self) -> None:
        assert normalize_signal_modules("a;b;c", ["x"]) == ["a", "b", "c"]

    def test_deduplication(self) -> None:
        assert normalize_signal_modules(["a", "b", "a"], ["x"]) == ["a", "b"]

    def test_empty_falls_back_to_default(self) -> None:
        assert normalize_signal_modules("", ["x"]) == ["x"]
        assert normalize_signal_modules([], ["y"]) == ["y"]

    def test_empty_string_with_spaces(self) -> None:
        assert normalize_signal_modules("   ", ["default"]) == ["default"]


# ---------------------------------------------------------------------------
# buy_filter functions
# ---------------------------------------------------------------------------

class TestBuyFilterPriceAboveMA:
    def test_ma20_passes_above(self) -> None:
        signal = {"calc_details": {"price": 15.0, "ma_mid": 10.0}}
        assert buy_filter_price_above_ma20(signal, {}) is True

    def test_ma20_fails_below(self) -> None:
        signal = {"calc_details": {"price": 8.0, "ma_mid": 10.0}}
        assert buy_filter_price_above_ma20(signal, {}) is False

    def test_ma60_passes_above(self) -> None:
        signal = {"calc_details": {"price": 50.0, "ma60": 45.0}}
        assert buy_filter_price_above_ma60(signal, {}) is True

    def test_ma60_fails_zero_ma(self) -> None:
        signal = {"calc_details": {"price": 50.0, "ma60": 0.0}}
        assert buy_filter_price_above_ma60(signal, {}) is False

    def test_ma200_passes(self) -> None:
        signal = {"calc_details": {"price": 100.0, "ma200": 90.0}}
        assert buy_filter_price_above_ma200(signal, {}) is True


class TestBuyFilterTrendScoreMax:
    def test_passes_below_max(self) -> None:
        signal = {"trend_score": 15.0}
        cfg = {"max_entry_trend_score": 20.0}
        assert buy_filter_trend_score_max(signal, cfg) is True

    def test_fails_above_max(self) -> None:
        signal = {"trend_score": 25.0}
        cfg = {"max_entry_trend_score": 20.0}
        assert buy_filter_trend_score_max(signal, cfg) is False

    def test_fails_when_max_not_set(self) -> None:
        signal = {"trend_score": 10.0}
        assert buy_filter_trend_score_max(signal, {}) is False


# ---------------------------------------------------------------------------
# sell_signal functions
# ---------------------------------------------------------------------------

class TestSellSignalHardStop:
    def test_triggers_below(self) -> None:
        signal = {"calc_details": {"price": 9.0}}
        state = {"hard_stop_price": 10.0}
        triggered, reason, _ = sell_signal_hard_stop(signal, {}, state, {})
        assert triggered is True
        assert reason == "hard_stop"

    def test_not_triggered_above(self) -> None:
        signal = {"calc_details": {"price": 11.0}}
        state = {"hard_stop_price": 10.0}
        triggered, reason, _ = sell_signal_hard_stop(signal, {}, state, {})
        assert triggered is False


class TestSellSignalChandelierStop:
    def test_triggers_below(self) -> None:
        signal = {"calc_details": {"price": 9.0}}
        state = {"chandelier_stop_price": 10.0}
        triggered, reason, _ = sell_signal_chandelier_stop(signal, {}, state, {})
        assert triggered is True

    def test_not_triggered_above(self) -> None:
        signal = {"calc_details": {"price": 11.0}}
        state = {"chandelier_stop_price": 10.0}
        triggered, reason, _ = sell_signal_chandelier_stop(signal, {}, state, {})
        assert triggered is False


class TestSellSignalMABreakdownMax:
    def test_triggers_below_max_ma(self) -> None:
        signal = {"calc_details": {"price": 42.0, "ma30": 50.0, "ma40": 45.0}}
        triggered, reason, updates = sell_signal_ma_breakdown_max(signal, {}, {}, {})
        assert triggered is True
        assert "ma30_breakdown" in reason

    def test_not_triggered_above_all(self) -> None:
        signal = {"calc_details": {"price": 60.0, "ma30": 50.0, "ma40": 45.0, "ma60": 40.0}}
        triggered, _, _ = sell_signal_ma_breakdown_max(signal, {}, {}, {})
        assert triggered is False

    def test_respects_enable_ma_exit_false(self) -> None:
        signal = {"calc_details": {"price": 42.0, "ma30": 50.0}}
        cfg = {"enable_ma_exit": False}
        triggered, _, _ = sell_signal_ma_breakdown_max(signal, {}, {}, cfg)
        assert triggered is False


# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------

class TestRegistries:
    def test_buy_filter_registry_has_expected_keys(self) -> None:
        assert "price_above_ma20" in BUY_FILTER_REGISTRY
        assert "price_above_ma60" in BUY_FILTER_REGISTRY
        assert "price_above_ma200" in BUY_FILTER_REGISTRY
        assert "trend_score_max" in BUY_FILTER_REGISTRY

    def test_sell_signal_registry_has_expected_keys(self) -> None:
        assert "hard_stop" in SELL_SIGNAL_REGISTRY
        assert "chandelier_stop" in SELL_SIGNAL_REGISTRY
        assert "ma_breakdown_max" in SELL_SIGNAL_REGISTRY

    def test_default_constants(self) -> None:
        assert DEFAULT_MOMENTUM_BUY_FILTERS == ["price_above_ma20"]
        assert DEFAULT_MOMENTUM_SELL_SIGNALS == ["hard_stop", "chandelier_stop"]
