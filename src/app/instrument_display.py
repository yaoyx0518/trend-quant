from __future__ import annotations

import re
from pathlib import Path

import yaml

ETF_SUFFIX_RE = re.compile(r"\s*ETF\s*$", flags=re.IGNORECASE)


def strip_etf_suffix(name: str | None) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    return ETF_SUFFIX_RE.sub("", text).strip()


def symbol_to_code(symbol: str | None) -> str:
    text = str(symbol or "").strip().upper()
    if "." in text:
        text = text.split(".", 1)[0]
    return text


def format_symbol_display(symbol: str | None, name: str | None = None) -> str:
    code = symbol_to_code(symbol)
    cleaned_name = strip_etf_suffix(name)
    if cleaned_name and code:
        return f"{cleaned_name}（{code}）"
    return cleaned_name or code


def load_instrument_name_map(path: str = "config/instruments.yaml") -> dict[str, str]:
    p = Path(path)
    if not p.exists():
        return {}

    payload = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    instruments = payload.get("instruments", []) if isinstance(payload, dict) else []
    if not isinstance(instruments, list):
        return {}

    out: dict[str, str] = {}
    for item in instruments:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol == "":
            continue
        out[symbol] = strip_etf_suffix(str(item.get("name", "") or ""))
    return out


def build_symbol_display(symbol: str | None, name_map: dict[str, str] | None = None) -> str:
    normalized = str(symbol or "").strip().upper()
    map_name = ""
    if name_map is not None:
        map_name = str(name_map.get(normalized, "") or "")
    return format_symbol_display(normalized, map_name)
