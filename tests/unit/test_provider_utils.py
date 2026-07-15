"""Unit tests for data.provider_utils — symbol normalization and OHLCV standardisation."""

from __future__ import annotations

import pandas as pd
import pytest

from data.provider_utils import normalize_symbol, safe_float, standardize_ohlcv, parse_minute_period


class TestNormalizeSymbol:
    def test_strips_exchange_suffix(self) -> None:
        assert normalize_symbol("510300.SS") == "510300"
        assert normalize_symbol("300760.SZ") == "300760"
        assert normalize_symbol("600276.SH") == "600276"

    def test_no_suffix_passes_through(self) -> None:
        assert normalize_symbol("510300") == "510300"

    def test_empty_string(self) -> None:
        assert normalize_symbol("") == ""


class TestSafeFloat:
    def test_normal_value(self) -> None:
        assert safe_float(3.14) == 3.14

    def test_none_returns_default(self) -> None:
        assert safe_float(None) is None
        assert safe_float(None, default=0.0) == 0.0

    def test_nan_returns_none(self) -> None:
        import math
        assert safe_float(float("nan")) is None


class TestStandardizeOhlcv:
    def test_renames_columns(self) -> None:
        df = pd.DataFrame({
            "trade_date": ["2025-01-01"],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "vol": [1_000_000],
            "amount": [10_500_000],
        })
        result = standardize_ohlcv(df, "TEST")
        assert "time" in result.columns
        assert "volume" in result.columns
        assert "open" in result.columns

    def test_sorts_by_time(self) -> None:
        df = pd.DataFrame({
            "trade_date": ["2025-01-03", "2025-01-01", "2025-01-02"],
            "open": [10.0, 10.0, 10.0],
            "high": [11.0, 11.0, 11.0],
            "low": [9.0, 9.0, 9.0],
            "close": [10.5, 10.5, 10.5],
            "vol": [1_000_000, 1_000_000, 1_000_000],
            "amount": [10_500_000, 10_500_000, 10_500_000],
        })
        result = standardize_ohlcv(df, "TEST")
        times = result["time"].tolist()
        assert times == sorted(times)


class TestParseMinutePeriod:
    def test_standard_periods(self) -> None:
        assert parse_minute_period("30m") == "30"
        assert parse_minute_period("5m") == "5"
        assert parse_minute_period("60m") == "60"

    def test_numeric_only(self) -> None:
        assert parse_minute_period("30") == "30"
