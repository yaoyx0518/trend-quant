"""Backward-compatible shim — canonical home is core.display."""

from core.display import (  # noqa: F401
    build_symbol_display,
    format_symbol_display,
    load_instrument_name_map,
    strip_etf_suffix,
    symbol_to_code,
)
