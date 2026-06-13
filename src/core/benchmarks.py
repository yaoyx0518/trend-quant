from __future__ import annotations

from dataclasses import dataclass


BENCHMARK_MODE_EQUAL_WEIGHT = "equal_weight_pool"
BENCHMARK_MODE_CUSTOM_SYMBOL = "symbol"
DEFAULT_BENCHMARK_SYMBOL = "512500.SS"


@dataclass(frozen=True)
class BenchmarkOption:
    mode: str
    label: str
    symbol: str = ""
    instrument_name: str = ""


EQUAL_WEIGHT_BENCHMARK = BenchmarkOption(
    mode=BENCHMARK_MODE_EQUAL_WEIGHT,
    label="所有标的等权持有",
)

INDEX_BENCHMARKS: tuple[BenchmarkOption, ...] = (
    BenchmarkOption(
        mode="csi500",
        label="中证500指数",
        symbol="510500.SS",
        instrument_name="中证500ETF南方",
    ),
    BenchmarkOption(
        mode="chinext",
        label="创业板指数",
        symbol="159915.SZ",
        instrument_name="创业板ETF易方达",
    ),
)

CUSTOM_SYMBOL_BENCHMARK = BenchmarkOption(
    mode=BENCHMARK_MODE_CUSTOM_SYMBOL,
    label="单标的（买入并持有）",
    symbol=DEFAULT_BENCHMARK_SYMBOL,
)

BENCHMARK_OPTIONS: tuple[BenchmarkOption, ...] = (
    EQUAL_WEIGHT_BENCHMARK,
    *INDEX_BENCHMARKS,
    CUSTOM_SYMBOL_BENCHMARK,
)

COMPARISON_BENCHMARKS: tuple[BenchmarkOption, ...] = (
    EQUAL_WEIGHT_BENCHMARK,
    *INDEX_BENCHMARKS,
)

VALID_BENCHMARK_MODES = {item.mode for item in BENCHMARK_OPTIONS}
INDEX_BENCHMARK_BY_MODE = {item.mode: item for item in INDEX_BENCHMARKS}


def normalize_benchmark_mode(mode: str) -> str:
    normalized = str(mode or BENCHMARK_MODE_EQUAL_WEIGHT).strip().lower()
    if normalized in VALID_BENCHMARK_MODES:
        return normalized
    return BENCHMARK_MODE_EQUAL_WEIGHT


def benchmark_symbol_for_mode(mode: str, custom_symbol: str = "") -> str:
    normalized_mode = normalize_benchmark_mode(mode)
    if normalized_mode in INDEX_BENCHMARK_BY_MODE:
        return INDEX_BENCHMARK_BY_MODE[normalized_mode].symbol
    if normalized_mode == BENCHMARK_MODE_CUSTOM_SYMBOL:
        return str(custom_symbol or DEFAULT_BENCHMARK_SYMBOL).strip().upper()
    return ""


def benchmark_label_for_mode(mode: str, custom_symbol: str = "") -> str:
    normalized_mode = normalize_benchmark_mode(mode)
    if normalized_mode == BENCHMARK_MODE_EQUAL_WEIGHT:
        return EQUAL_WEIGHT_BENCHMARK.label
    if normalized_mode in INDEX_BENCHMARK_BY_MODE:
        option = INDEX_BENCHMARK_BY_MODE[normalized_mode]
        return f"{option.label}（{option.symbol}）"
    symbol = benchmark_symbol_for_mode(normalized_mode, custom_symbol)
    return f"{CUSTOM_SYMBOL_BENCHMARK.label}:{symbol}" if symbol else CUSTOM_SYMBOL_BENCHMARK.label


def benchmark_market_symbols() -> list[str]:
    return [item.symbol for item in INDEX_BENCHMARKS if item.symbol]


def benchmark_instruments() -> list[dict]:
    return [
        {
            "symbol": item.symbol,
            "name": item.instrument_name or item.label,
            "benchmark_mode": item.mode,
        }
        for item in INDEX_BENCHMARKS
        if item.symbol
    ]
