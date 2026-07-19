"""MCP server for trend-quant.

Exposes 5 tools to external agents via MCP SSE transport:

1. **trend_dashboard** -- 标的看板: multi-symbol trend dashboard grouped by
   three-level category hierarchy (EOD daily bars).
2. **intraday_dashboard** -- 实时看板: same structure as trend_dashboard but
   computed from real-time quotes (trading days 9:30-15:00, lunch break
   included).
3. **symbol_detail** -- 标的查看: historical OHLCV + full indicator suite for
   a single symbol, with an optional real-time intraday overlay.
4. **calc_stop_loss** -- 辅助计算: hard-stop and chandelier-stop prices for
   a given buy entry.
5. **list_instruments** -- 标的列表: searchable / filterable instrument
   catalogue.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from mcp.server.fastmcp import FastMCP

from app.instrument_display import format_symbol_display
from app.routers.market_view import (
    _config_name_map,
    _normalize_symbol,
    _trend_config,
    compute_market_indicators,
)
from app.routers.subject_market import build_subject_dashboard_payload
from core.calendar import is_realtime_available, is_trading_day
from core.strategy_config import get_strategy_config
from data.intraday_service import (
    build_intraday_dashboard,
    build_synthetic_bar,
    compute_intraday_trend_score,
)
from data.service import DataService
from data.storage.db import get_db
from strategy.indicators import atr
from strategy.trend_score_core import safe_float

# ---------------------------------------------------------------------------
# Server instance
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "trend-quant",
    transport_security={"enable_dns_rebinding_protection": False},
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_strategy_config() -> dict:
    return get_strategy_config()


def _load_instruments_raw() -> list[dict]:
    import sqlite3

    try:
        return [dict(item) for item in get_db().list_instrument_metadata()]
    except (RuntimeError, sqlite3.Error):
        return []  # database unavailable


def _instrument_metadata_map(instruments: list[dict]) -> dict[str, dict]:
    return {
        str(item.get("symbol", "")).strip().upper(): item
        for item in instruments
        if str(item.get("symbol", "")).strip().upper()
    }


def _category_path(meta: dict | None) -> str:
    if not meta:
        return ""
    parts = [
        str(meta.get("category_l1") or "").strip(),
        str(meta.get("category_l2") or "").strip(),
        str(meta.get("category_l3") or "").strip(),
    ]
    return "-".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Dashboard cache (same strategy as subject_market.py)
# ---------------------------------------------------------------------------

_dashboard_cache: tuple[tuple[str, int, str], dict] | None = None


# ---------------------------------------------------------------------------
# Tool 1 -- trend_dashboard
# ---------------------------------------------------------------------------

@mcp.tool()
def trend_dashboard() -> dict:
    """获取标的看板数据（基于日K线，不含当天盘中实时数据）。

    Returns all ETF instruments grouped by a three-level category
    hierarchy (L1/L2/L3), each with:
    - 最新趋势值 (trend_score)
    - 趋势值 MA5 (trend_ma5, the primary ranking metric)
    - 同级强度百分位 (strength)
    - 日涨跌幅 / 5日 / 20日 / 60日涨跌幅
    - 趋势相位检测 (上升 / 下降 / 震荡)
    - 历史趋势值 MA5 序列 (trend_history)

    数据来自本地日K库，最新一根K线通常是上一个交易日。
    如需当天实时数据请使用 intraday_dashboard。
    """
    global _dashboard_cache
    db = get_db()
    revision = db.get_market_dashboard_revision()
    if _dashboard_cache is not None and _dashboard_cache[0] == revision:
        return _dashboard_cache[1]
    payload = build_subject_dashboard_payload(db)
    _dashboard_cache = (revision, payload)
    return payload


# ---------------------------------------------------------------------------
# Tool 2 -- intraday_dashboard (real-time)
# ---------------------------------------------------------------------------

@mcp.tool()
def intraday_dashboard(category: str = "") -> dict:
    """获取实时标的看板（基于当天实时报价，含盘中趋势值）。

    仅在交易日 9:30-15:00（含午间休盘 11:30-13:00）可用；
    非交易时段请使用 trend_dashboard 获取日K看板。

    Args:
        category: 可选，按分类筛选（匹配 L1/L2/L3），如 "宽基"、"行业"、
            "跨境"。不传则计算全部标的（600+，可能需要 1 分钟以上，
            建议按需用 category 缩小范围）。

    Returns:
        与 trend_dashboard 相同的三级分类结构，另含:
        - is_intraday: True
        - intraday_ts: 计算时间戳
        - 每个标的的 daily_change_pct 为实时涨跌幅
    """
    now = datetime.now()
    if not is_trading_day(now.date()):
        return {
            "ok": False,
            "error": "今日非交易日，无实时数据；请使用 trend_dashboard 获取日K看板",
        }
    if not is_realtime_available(now):
        return {
            "ok": False,
            "error": "当前非实时行情时段（交易日 9:30-15:00，含午间休盘）；请使用 trend_dashboard 获取日K看板",
        }

    db = get_db()
    symbols = db.list_market_symbols(price_mode="qfq")
    if not symbols:
        return {"ok": False, "error": "本地无日K数据"}

    # Filter to fully classified instruments (mirrors the web intraday job).
    metadata_map = db.get_instrument_metadata_map()
    classified = [
        s for s in symbols
        if s in metadata_map
        and str(metadata_map[s].get("category_l1", "")).strip()
        and str(metadata_map[s].get("category_l2", "")).strip()
        and str(metadata_map[s].get("category_l3", "")).strip()
    ]

    # Optional category filter (match any level, case-insensitive).
    if category.strip():
        kw = category.strip().lower()
        classified = [
            s for s in classified
            if kw in str(metadata_map[s].get("category_l1", "")).lower()
            or kw in str(metadata_map[s].get("category_l2", "")).lower()
            or kw in str(metadata_map[s].get("category_l3", "")).lower()
        ]

    if not classified:
        return {"ok": False, "error": "无符合条件的标的（需完整三级分类）"}

    payload = build_intraday_dashboard(
        classified, db, DataService(), _trend_config()
    )
    payload["ok"] = True
    payload["requested_category"] = category.strip() or None
    return payload


# ---------------------------------------------------------------------------
# Tool 3 -- symbol_detail
# ---------------------------------------------------------------------------

@mcp.tool()
def symbol_detail(symbol: str, days: int = 60, rsi_period: int = 14, intraday: bool = False) -> dict:
    """获取指定标的的历史日K线、趋势指标和全套技术指标。

    Args:
        symbol: 标的代码，如 510300.SS 或 510300
        days: 返回最近多少天的数据，默认 60
        rsi_period: RSI 计算周期，默认 14
        intraday: 是否叠加当天实时数据，默认 False。
            仅交易日 9:30-15:00（含午间休盘）生效：追加一根由实时报价
            合成的当日K线，并在 indicators.trend_intraday 中返回盘中
            趋势值快照；非交易时段或实时行情获取失败时静默回退为日K数据。

    Returns:
        包含 dates、candles(OHLC)、volumes、indicators 的完整数据。
        indicators 包含: trend(score/ma/price_direction/confidence),
        ma, atr, bias, boll, macd, rsi。
        meta.is_intraday 标记是否包含实时数据。
    """
    symbol = _normalize_symbol(symbol)
    if not symbol:
        return {"ok": False, "error": "无效的标的代码"}

    db = get_db()
    df = db.load_market_data(symbol)
    if df.empty:
        return {"ok": False, "error": f"未找到 {symbol} 的数据，请确认代码正确且数据已入库"}

    # Load enough bars for indicator bootstrapping, then tail to requested days.
    # Trend-score needs max(n_long, atr_period) + 2 ≈ 22 bars minimum.
    MIN_BARS = 30
    requested = max(int(days), 1)
    df = df.tail(max(requested, MIN_BARS)).copy()

    name_map = _config_name_map()
    instruments = _load_instruments_raw()
    metadata_map = _instrument_metadata_map(instruments)
    name = name_map.get(symbol, "")
    metadata = metadata_map.get(symbol)

    rsi_period = max(2, int(rsi_period or 14))
    trend_cfg = _trend_config()
    indicators = compute_market_indicators(df, trend_cfg=trend_cfg, rsi_period=rsi_period)

    # Tail output arrays to the requested number of days
    def _tail(values: list, n: int) -> list:
        return values[-n:] if len(values) > n else values

    def _float_list(series_like) -> list:
        return [round(float(v), 4) if pd.notna(v) else None for v in series_like]

    n = min(requested, len(df))
    dates_out = [str(d.date()) for d in df["time"]][-n:]

    payload = {
        "ok": True,
        "symbol": symbol,
        "name": name,
        "display_name": format_symbol_display(symbol, name),
        "category": _category_path(metadata),
        "category_l1": str((metadata or {}).get("category_l1") or ""),
        "category_l2": str((metadata or {}).get("category_l2") or ""),
        "category_l3": str((metadata or {}).get("category_l3") or ""),
        "meta": db.get_market_data_summary(symbol),
        "dates": dates_out,
        "candles": {
            "open": _tail(_float_list(df["open"]), n),
            "high": _tail(_float_list(df["high"]), n),
            "low": _tail(_float_list(df["low"]), n),
            "close": _tail(_float_list(df["close"]), n),
        },
        "volumes": _tail(
            [int(v) if pd.notna(v) else None for v in df.get("volume", pd.Series())], n
        ),
        "indicators": indicators,
    }
    payload["meta"]["is_intraday"] = False

    # --- Intraday overlay (mirrors market_view.get_market_daily) ----------
    # Gate on is_realtime_available (not is_trading_time) so the midday
    # lunch break still serves an intraday snapshot from live quotes.
    if intraday and is_realtime_available():
        try:
            quote = DataService().fetch_latest_quote(symbol)
            if quote and quote.get("price") is not None:
                hist = df.copy()
                hist["time"] = pd.to_datetime(hist["time"], errors="coerce")
                hist = (
                    hist.dropna(subset=["time", "open", "high", "low", "close"])
                    .sort_values("time")
                    .reset_index(drop=True)
                )
                intraday_result = compute_intraday_trend_score(hist, quote, trend_cfg)
                if intraday_result.get("ok"):
                    prev_vol = safe_float(hist["volume"].iloc[-1], 0.0) if len(hist) > 0 else 0.0
                    synth = build_synthetic_bar(quote, prev_vol)
                    payload["dates"].append(str(datetime.now().date()))
                    payload["candles"]["open"].append(round(float(synth["open"]), 4))
                    payload["candles"]["high"].append(round(float(synth["high"]), 4))
                    payload["candles"]["low"].append(round(float(synth["low"]), 4))
                    payload["candles"]["close"].append(round(float(synth["close"]), 4))
                    payload["volumes"].append(int(synth["volume"]))
                    payload["indicators"]["trend_intraday"] = {
                        "score": intraday_result["trend_score"],
                        "price_direction": intraday_result["price_direction"],
                        "confidence": intraday_result["confidence"],
                        "atr": intraday_result["atr"],
                        "price": intraday_result["price"],
                        "ma_mid": intraday_result["ma_mid"],
                        "calc_details": intraday_result.get("calc_details", {}),
                    }
                    payload["meta"]["is_intraday"] = True
                    payload["meta"]["intraday_ts"] = datetime.now().isoformat()
        except Exception:
            # Silently fall back to EOD data if intraday fetch fails.
            pass

    return payload


# ---------------------------------------------------------------------------
# Tool 4 -- calc_stop_loss
# ---------------------------------------------------------------------------

@mcp.tool()
def calc_stop_loss(symbol: str, buy_date: str, buy_price: float) -> dict:
    """计算给定买入的硬止损价和吊灯止损价。

    硬止损公式: 买入价 − 买入当日 ATR(20) × hard_stop_atr_mul (默认 1.5)
    吊灯止损公式: 买入以来最高价 − 最新 ATR(20) × chandelier_stop_atr_mul (默认 2.5)

    Args:
        symbol: 标的代码，如 510300.SS
        buy_date: 买入日期，格式 YYYY-MM-DD
        buy_price: 买入均价

    Returns:
        硬止损价、吊灯止损价、ATR 参数、距买入价的百分比等。
    """
    symbol = _normalize_symbol(symbol)
    if not symbol:
        return {"ok": False, "error": "无效的标的代码"}

    db = get_db()
    df = db.load_market_data(symbol)
    if df.empty:
        return {"ok": False, "error": f"未找到 {symbol} 的数据"}

    strategy_cfg = _load_strategy_config()
    atr_period = int(strategy_cfg.get("atr_period", 20))
    hard_stop_mul = float(strategy_cfg.get("hard_stop_atr_mul_default", 1.5))
    chandelier_mul = float(strategy_cfg.get("chandelier_stop_atr_mul", 2.5))

    # Per-instrument stop_atr_mul override
    instruments = _load_instruments_raw()
    for item in instruments:
        if item.get("symbol", "").strip().upper() == symbol:
            if "stop_atr_mul" in item:
                hard_stop_mul = float(item["stop_atr_mul"])
            break

    atr_series = atr(df, period=atr_period)
    if atr_series.empty:
        return {"ok": False, "error": "数据不足，无法计算 ATR"}

    current_atr = safe_float(atr_series.iloc[-1], 0.0)
    if current_atr <= 0:
        return {"ok": False, "error": "ATR 值为 0，数据异常"}

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    buy_ts = pd.Timestamp(buy_date)

    # ATR at buy date (look back up to and including buy_date)
    atr_at_buy = current_atr
    mask_until = df["time"] <= buy_ts
    if mask_until.any():
        subset = atr_series[mask_until.values]
        if not subset.empty and pd.notna(subset.iloc[-1]):
            atr_at_buy = safe_float(subset.iloc[-1], current_atr)

    # Highest price since buy date (inclusive)
    highs = pd.to_numeric(df["high"], errors="coerce")
    latest_price = safe_float(pd.to_numeric(df["close"], errors="coerce").iloc[-1], 0.0)
    highest_since_buy = latest_price
    mask_since = df["time"] >= buy_ts
    if mask_since.any():
        since_highs = highs[mask_since]
        if not since_highs.empty and since_highs.notna().any():
            highest_since_buy = safe_float(since_highs.max(), latest_price)

    # Calculate stop prices
    hard_stop_price = round(buy_price - hard_stop_mul * atr_at_buy, 4)
    chandelier_stop_price = round(highest_since_buy - chandelier_mul * current_atr, 4)

    hard_stop_pct = round((hard_stop_price / buy_price - 1) * 100, 2)
    chandelier_pct = (
        round((chandelier_stop_price / highest_since_buy - 1) * 100, 2)
        if highest_since_buy > 0
        else 0.0
    )

    return {
        "ok": True,
        "symbol": symbol,
        "buy_price": buy_price,
        "buy_date": buy_date,
        "hard_stop_price": hard_stop_price,
        "hard_stop_pct": hard_stop_pct,
        "hard_stop_atr_mul": hard_stop_mul,
        "chandelier_stop_price": chandelier_stop_price,
        "chandelier_stop_pct_from_high": chandelier_pct,
        "chandelier_stop_atr_mul": chandelier_mul,
        "atr_at_buy": round(atr_at_buy, 4),
        "current_atr": round(current_atr, 4),
        "highest_since_buy": round(highest_since_buy, 4),
        "latest_price": round(latest_price, 4),
    }


# ---------------------------------------------------------------------------
# Tool 5 -- list_instruments
# ---------------------------------------------------------------------------

@mcp.tool()
def list_instruments(
    category: str = "",
    keyword: str = "",
    enabled_only: bool = True,
) -> dict:
    """列出所有可用的 ETF 标的，支持按分类和关键词筛选。

    Args:
        category: 按分类筛选（匹配 L1/L2/L3），如 "宽基"、"行业"、"跨境"
        keyword: 按代码或名称模糊搜索
        enabled_only: 是否仅返回启用的标的，默认 True

    Returns:
        标的列表，包含代码、名称、三级分类、数据范围、启用状态。
    """
    instruments = _load_instruments_raw()
    db = get_db()

    result: list[dict] = []
    for item in instruments:
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue

        if enabled_only and not item.get("enabled", True):
            continue

        cat_l1 = str(item.get("category_l1") or "")
        cat_l2 = str(item.get("category_l2") or "")
        cat_l3 = str(item.get("category_l3") or "")
        name = str(item.get("name") or "")

        # Filter by category keyword (match any level)
        if category:
            kw = category.strip().lower()
            if not (kw in cat_l1.lower() or kw in cat_l2.lower() or kw in cat_l3.lower()):
                continue

        # Filter by symbol / name keyword
        if keyword:
            kw = keyword.strip().lower()
            if not (kw in symbol.lower() or kw in name.lower()):
                continue

        db_summary = db.get_market_data_summary(symbol)

        result.append(
            {
                "symbol": symbol,
                "name": name,
                "category_l1": cat_l1,
                "category_l2": cat_l2,
                "category_l3": cat_l3,
                "enabled": bool(item.get("enabled", True)),
                "data_rows": db_summary.get("rows", 0),
                "data_start": str(db_summary.get("start", ""))
                if db_summary.get("start")
                else None,
                "data_end": str(db_summary.get("end", ""))
                if db_summary.get("end")
                else None,
            }
        )

    return {"ok": True, "count": len(result), "instruments": result}
