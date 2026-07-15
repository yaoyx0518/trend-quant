"""Unit tests for core.benchmarks — benchmark options and utilities."""

from __future__ import annotations

from core.benchmarks import (
    normalize_benchmark_mode,
    benchmark_symbol_for_mode,
    benchmark_label_for_mode,
    benchmark_market_symbols,
    benchmark_instruments,
    BENCHMARK_MODE_EQUAL_WEIGHT,
)


class TestNormalizeBenchmarkMode:
    def test_known_modes(self) -> None:
        assert normalize_benchmark_mode("equal_weight_pool") == "equal_weight_pool"
        assert normalize_benchmark_mode("csi500") == "csi500"

    def test_case_insensitive(self) -> None:
        assert normalize_benchmark_mode("CSI500") == "csi500"

    def test_unknown_falls_back(self) -> None:
        assert normalize_benchmark_mode("unknown") == BENCHMARK_MODE_EQUAL_WEIGHT

    def test_none_falls_back(self) -> None:
        assert normalize_benchmark_mode("") == BENCHMARK_MODE_EQUAL_WEIGHT


class TestBenchmarkSymbolForMode:
    def test_index_mode(self) -> None:
        assert benchmark_symbol_for_mode("csi500") == "510500.SS"
        assert benchmark_symbol_for_mode("chinext") == "159915.SZ"

    def test_equal_weight_returns_empty(self) -> None:
        assert benchmark_symbol_for_mode("equal_weight_pool") == ""


class TestBenchmarkLabelForMode:
    def test_returns_label(self) -> None:
        label = benchmark_label_for_mode("csi500")
        assert "中证500" in label
        assert "510500" in label


class TestBenchmarkUtilities:
    def test_market_symbols(self) -> None:
        symbols = benchmark_market_symbols()
        assert "510500.SS" in symbols
        assert "159915.SZ" in symbols

    def test_instruments(self) -> None:
        instruments = benchmark_instruments()
        assert len(instruments) >= 2
        assert instruments[0]["symbol"]
