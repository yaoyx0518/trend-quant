from __future__ import annotations

from datetime import datetime
from typing import Iterable

import pandas as pd


def normalize_symbol(symbol: str) -> str:
    value = (symbol or "").strip().upper()
    for suffix in (".SS", ".SZ", ".SH"):
        if value.endswith(suffix):
            return value.split(".")[0]
    return value


def safe_float(value: object, default: float | None = None) -> float | None:
    if value is None:
        return default
    text = str(value).strip().replace(",", "")
    if text == "" or text.lower() in {"none", "nan", "-"}:
        return default
    try:
        return float(text)
    except Exception:
        return default


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def _first_existing(columns: Iterable[str], aliases: list[str]) -> str | None:
    lower_map = {c.lower(): c for c in columns}
    for alias in aliases:
        key = alias.lower()
        if key in lower_map:
            return lower_map[key]
    return None


def standardize_ohlcv(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "amount", "symbol"])

    data = _normalize_columns(df)
    cols = list(data.columns)

    time_col = _first_existing(cols, ["time", "datetime", "date", "日期", "时间"])
    open_col = _first_existing(cols, ["open", "开盘"])
    high_col = _first_existing(cols, ["high", "最高"])
    low_col = _first_existing(cols, ["low", "最低"])
    close_col = _first_existing(cols, ["close", "收盘", "最新价", "最新"])
    volume_col = _first_existing(cols, ["volume", "成交量"])
    amount_col = _first_existing(cols, ["amount", "成交额"])

    normalized = pd.DataFrame()
    if time_col:
        normalized["time"] = pd.to_datetime(data[time_col], errors="coerce")
    else:
        normalized["time"] = pd.to_datetime(pd.Series([datetime.now()] * len(data)), errors="coerce")

    for out_col, in_col in [
        ("open", open_col),
        ("high", high_col),
        ("low", low_col),
        ("close", close_col),
        ("volume", volume_col),
        ("amount", amount_col),
    ]:
        if in_col:
            normalized[out_col] = pd.to_numeric(data[in_col], errors="coerce")
        else:
            normalized[out_col] = pd.NA

    normalized["symbol"] = symbol
    normalized = normalized.dropna(subset=["time"]).drop_duplicates(subset=["time"]).sort_values("time")
    normalized = normalized.reset_index(drop=True)
    return normalized


def parse_minute_period(period: str) -> str:
    value = (period or "30").lower().replace("m", "")
    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or "30"
