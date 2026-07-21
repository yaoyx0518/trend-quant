"""Unit tests for core.symbols — the single symbol-normalization implementation."""

from __future__ import annotations

from core.symbols import normalize_symbol, symbol_suffix, symbol_to_code


class TestNormalizeSymbol:
    def test_bare_shanghai_codes(self) -> None:
        assert normalize_symbol("510300") == "510300.SS"
        assert normalize_symbol("688072") == "688072.SS"
        assert normalize_symbol("600519") == "600519.SS"

    def test_bare_shenzhen_codes(self) -> None:
        assert normalize_symbol("159915") == "159915.SZ"
        assert normalize_symbol("000001") == "000001.SZ"
        assert normalize_symbol("301516") == "301516.SZ"

    def test_suffixed_passthrough_uppercased(self) -> None:
        assert normalize_symbol("510300.ss") == "510300.SS"
        assert normalize_symbol("159915.SZ") == "159915.SZ"

    def test_sh_suffix_normalized_to_ss(self) -> None:
        assert normalize_symbol("510300.SH") == "510300.SS"
        assert normalize_symbol("600519.sh") == "600519.SS"

    def test_whitespace_and_case(self) -> None:
        assert normalize_symbol("  510300  ") == "510300.SS"

    def test_non_standard_input_passthrough(self) -> None:
        assert normalize_symbol("ABC") == "ABC"
        assert normalize_symbol("") == ""
        assert normalize_symbol(None) == ""  # type: ignore[arg-type]


class TestSymbolToCode:
    def test_strips_suffix(self) -> None:
        assert symbol_to_code("510300.SS") == "510300"
        assert symbol_to_code("159915.SZ") == "159915"

    def test_bare_code_passthrough(self) -> None:
        assert symbol_to_code("510300") == "510300"
        assert symbol_to_code("") == ""


class TestSymbolSuffix:
    def test_returns_suffix(self) -> None:
        assert symbol_suffix("510300.SS") == "SS"
        assert symbol_suffix("159915.SZ") == "SZ"

    def test_unsuffixed_returns_empty(self) -> None:
        assert symbol_suffix("510300") == ""
