"""Unit tests for strategy.entry_filters."""

from __future__ import annotations

import pytest

from strategy.entry_filters import (
    PriceAboveMa20Filter,
    PriceAboveMa60Filter,
    PriceAboveMa200Filter,
    TrendScoreMaxFilter,
    UnknownEntryFilter,
    build_entry_filters,
)


class TestPriceAboveMa20Filter:
    def test_passes_when_above(self) -> None:
        f = PriceAboveMa20Filter()
        signal = {"calc_details": {"price": 15.0, "ma_mid": 10.0}}
        result = f.evaluate(signal, {})
        assert result.passed is True

    def test_fails_when_below(self) -> None:
        f = PriceAboveMa20Filter()
        signal = {"calc_details": {"price": 8.0, "ma_mid": 10.0}}
        result = f.evaluate(signal, {})
        assert result.passed is False

    def test_fails_when_price_zero(self) -> None:
        f = PriceAboveMa20Filter()
        signal = {"calc_details": {"price": 0.0, "ma_mid": 10.0}}
        result = f.evaluate(signal, {})
        assert result.passed is False


class TestPriceAboveMa60Filter:
    def test_passes_when_above(self) -> None:
        f = PriceAboveMa60Filter()
        signal = {"calc_details": {"price": 50.0, "ma60": 45.0}}
        result = f.evaluate(signal, {})
        assert result.passed is True

    def test_fails_when_ma60_zero(self) -> None:
        f = PriceAboveMa60Filter()
        signal = {"calc_details": {"price": 50.0, "ma60": 0.0}}
        result = f.evaluate(signal, {})
        assert result.passed is False


class TestTrendScoreMaxFilter:
    def test_passes_when_below_max(self) -> None:
        f = TrendScoreMaxFilter()
        signal = {"trend_score": 15.0}
        cfg = {"max_entry_trend_score": 20.0}
        result = f.evaluate(signal, cfg)
        assert result.passed is True

    def test_fails_when_above_max(self) -> None:
        f = TrendScoreMaxFilter()
        signal = {"trend_score": 25.0}
        cfg = {"max_entry_trend_score": 20.0}
        result = f.evaluate(signal, cfg)
        assert result.passed is False

    def test_fails_when_max_is_zero(self) -> None:
        f = TrendScoreMaxFilter()
        signal = {"trend_score": 10.0}
        cfg = {"max_entry_trend_score": 0.0}
        result = f.evaluate(signal, cfg)
        assert result.passed is False


class TestBuildEntryFilters:
    def test_returns_registered_filters(self) -> None:
        built = build_entry_filters(["price_above_ma20", "trend_score_max"])
        assert len(built) == 2
        assert isinstance(built[0], PriceAboveMa20Filter)
        assert isinstance(built[1], TrendScoreMaxFilter)

    def test_returns_unknown_for_unrecognized(self) -> None:
        built = build_entry_filters(["nonexistent_filter"])
        assert len(built) == 1
        assert isinstance(built[0], UnknownEntryFilter)
        assert built[0].evaluate({}, {}).passed is False

    def test_empty_list(self) -> None:
        assert build_entry_filters([]) == []
