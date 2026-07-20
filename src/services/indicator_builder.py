"""Indicator precompute pipeline (P1.2).

Responsibilities:
- Full-symbol rebuild of the indicator caches from K-line history
  (vectorized, milliseconds per symbol). Full rebuild is deliberate:
  qfq adjustments retroactively rewrite history, so row-level
  incrementality is unsound (master plan D4).
- Default param-set registry with hash check (D3): a config or formula
  change marks the caches for a full rebuild.
- Dividend/adjustment detection (D9): the daily K-line update is
  append-only, so a corporate action silently breaks the stored series.
  We re-fetch the recent window and compare; mismatches trigger a full
  history re-pull of that symbol before its indicators are rebuilt.
- Pre-rebuild backup (D10): VACUUM INTO snapshot before full rebuilds.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from typing import Any

from audit.app_logger import get_logger
from core.indicators import INDICATOR_FORMULA_VERSION
from core.strategy_config import get_strategy_config
from core.trend import TREND_FORMULA_VERSION
from data.indicator_store import compute_indicator_frame, compute_trend_frame
from data.storage.db import get_db

logger = get_logger(__name__)

DIVIDEND_CHECK_BARS = 10


# ---------------------------------------------------------------------------
# Param-set registry (D3)
# ---------------------------------------------------------------------------


def normalized_params_json(cfg: dict[str, Any]) -> str:
    """Deterministic serialization (sorted keys, fixed float repr via json)."""
    return json.dumps(cfg, sort_keys=True, separators=(",", ":"), default=str)


def params_hash(cfg: dict[str, Any]) -> str:
    digest = hashlib.sha1(
        (str(TREND_FORMULA_VERSION) + "|" + normalized_params_json(cfg)).encode("utf-8")
    ).hexdigest()
    return digest[:12]


def default_param_set_needs_rebuild(cfg: dict[str, Any], db=None) -> bool:
    """True when the stored default param set no longer matches current config."""
    db = db or get_db()
    row = db.get_param_set("default")
    if row is None:
        return True
    current = normalized_params_json(cfg)
    return row["params_json"] != current or int(row["formula_version"]) != TREND_FORMULA_VERSION


def register_default_param_set(cfg: dict[str, Any], db=None) -> None:
    db = db or get_db()
    db.save_param_set("default", normalized_params_json(cfg), True, TREND_FORMULA_VERSION)


# ---------------------------------------------------------------------------
# Rebuild primitives
# ---------------------------------------------------------------------------


def rebuild_symbol(symbol: str, trend_cfg: dict, db=None) -> dict:
    """Full-symbol rebuild of both cache tables from stored K-lines."""
    db = db or get_db()
    symbol = str(symbol or "").strip().upper()
    df = db.load_market_data(symbol)
    if df.empty:
        return {"symbol": symbol, "status": "no_data", "rows": 0}
    ind_frame = compute_indicator_frame(df)
    trend_frame = compute_trend_frame(df, trend_cfg)
    ind_rows = db.save_indicator_daily(symbol, ind_frame, INDICATOR_FORMULA_VERSION)
    trend_rows = db.save_trend_daily(symbol, trend_frame, TREND_FORMULA_VERSION)
    return {"symbol": symbol, "status": "rebuilt", "rows": ind_rows, "trend_rows": trend_rows}


def rebuild_all(symbols: list[str] | None = None, trend_cfg: dict | None = None, db=None) -> dict:
    db = db or get_db()
    trend_cfg = trend_cfg or get_strategy_config()
    if symbols is None:
        symbols = sorted(db.list_market_symbols())
    rebuilt, failed = 0, 0
    for symbol in symbols:
        try:
            result = rebuild_symbol(symbol, trend_cfg, db=db)
            if result["status"] == "rebuilt":
                rebuilt += 1
        except Exception:
            failed += 1
            logger.exception("Indicator rebuild failed for %s", symbol)
    register_default_param_set(trend_cfg, db=db)
    return {"total": len(symbols), "rebuilt": rebuilt, "failed": failed}


def rebuild_if_needed(db=None) -> dict:
    """Startup check: rebuild everything when params/formula drifted."""
    db = db or get_db()
    cfg = get_strategy_config()
    if not default_param_set_needs_rebuild(cfg, db=db):
        return {"status": "up_to_date"}
    logger.info("Trend params or formula version changed — full indicator rebuild scheduled")
    db.backup_to()
    result = rebuild_all(trend_cfg=cfg, db=db)
    result["status"] = "rebuilt"
    return result


# ---------------------------------------------------------------------------
# Dividend / adjustment detection (D9)
# ---------------------------------------------------------------------------


def detect_adjustment_breaks(symbols: list[str], data_service, end_date: date, lookback: int = DIVIDEND_CHECK_BARS) -> list[str]:
    """Return symbols whose stored recent bars diverge from the vendor.

    The daily update is append-only; after a corporate action the vendor's
    qfq history is retroactively rewritten while our stored series is not.
    """
    broken: list[str] = []
    start_date = end_date - timedelta(days=lookback * 3)
    for symbol in symbols:
        try:
            stored = data_service.market_store.load_history(symbol)
            if stored.empty:
                continue
            fresh = data_service.fetch_daily_history(symbol, start=start_date, end=end_date, adjust="qfq")
            if fresh.empty:
                continue
            stored = stored.copy()
            fresh = fresh.copy()
            stored["time"] = stored["time"].astype(str).str[:10]
            fresh["time"] = fresh["time"].astype(str).str[:10]
            merged = stored.merge(fresh, on="time", suffixes=("_old", "_new"))
            if merged.empty:
                continue
            diff = (merged["close_old"] - merged["close_new"]).abs() / merged["close_new"].replace(0, 1)
            if bool((diff > 1e-6).any()):
                broken.append(symbol)
                logger.warning("Adjustment break detected for %s (max close diff %.4f%%)", symbol, float(diff.max()) * 100)
        except Exception:
            logger.exception("Adjustment check failed for %s", symbol)
    return broken


def repair_broken_symbols(symbols: list[str], data_service, start_date: date, end_date: date) -> list[dict]:
    """Full history re-pull for symbols with detected adjustment breaks."""
    results = []
    for symbol in symbols:
        try:
            result = data_service.backfill_daily_history(
                symbol=symbol, start_date=start_date, end_date=end_date, adjust="qfq"
            )
            results.append(result)
            logger.info("Re-pulled full history for %s after adjustment break: %s", symbol, result.get("status"))
        except Exception:
            logger.exception("Failed to re-pull history for %s", symbol)
    return results


def run_post_update_pipeline(settings, data_service, update_payload: dict, symbols: list[str], end_date: date, db=None) -> dict:
    """Post daily-update pipeline: dividend check → re-pull → indicator rebuild."""
    db = db or get_db()
    trend_cfg = get_strategy_config()
    symbols = [str(s).strip().upper() for s in symbols if s]
    if not symbols:
        symbols = sorted(db.list_market_symbols())

    broken = detect_adjustment_breaks(symbols, data_service, end_date)
    if broken:
        start_text = str(trend_cfg.get("backtest_start_primary", "2025-01-01"))
        start_date = date.fromisoformat(start_text)
        repair_broken_symbols(broken, data_service, start_date, end_date)

    updated = {
        str(r.get("symbol", "")).strip().upper()
        for r in update_payload.get("results", [])
        if isinstance(r, dict) and r.get("status") == "updated"
    }
    targets = sorted(updated | set(broken))
    if not targets:
        return {"status": "up_to_date", "dividend_breaks": broken, "rebuilt": 0}

    result = rebuild_all(symbols=targets, trend_cfg=trend_cfg, db=db)
    result["dividend_breaks"] = broken
    return result
