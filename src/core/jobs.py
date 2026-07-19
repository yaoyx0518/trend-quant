"""Scheduled jobs for the application.

Currently only the daily market data update (16:30 on trading days),
migrated from the retired signal engine's ``run_daily_update``.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import yaml

from audit.app_logger import get_logger
from core.benchmarks import benchmark_market_symbols
from core.calendar import is_trading_day
from core.settings import Settings
from data.service import DataService
from data.storage.runtime_store import RuntimeStore

logger = get_logger(__name__)


def _load_yaml(path: str) -> dict:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _pool_symbols() -> list[str]:
    """Enabled instruments from the metadata table plus benchmark symbols, deduped."""
    from data.storage.db import get_db

    import sqlite3

    try:
        instruments = [
            item
            for item in get_db().list_instrument_metadata()
            if item.get("enabled", 1) in (1, True)
        ]
    except (RuntimeError, sqlite3.Error):
        instruments = []  # database unavailable; fall back to benchmarks only

    symbols: list[str] = []
    seen: set[str] = set()
    for raw_symbol in [*(str(item.get("symbol")) for item in instruments), *benchmark_market_symbols()]:
        symbol = str(raw_symbol or "").strip().upper()
        if symbol == "" or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def daily_market_update_job(settings: Settings, data_service: DataService | None = None) -> dict:
    """Incrementally backfill daily K-line data for the whole instrument pool.

    Writes an advice record on non-trading days; on trading days the
    DataService itself persists ``advice/data_update_<date>.json``.
    """
    today = date.today()
    if not is_trading_day(today):
        payload = {
            "ts": datetime.now().isoformat(),
            "status": "skipped_non_trading_day",
            "results": [],
        }
        RuntimeStore().write_json(f"advice/data_update_{today.isoformat()}.json", payload)
        return payload

    strategy_cfg = _load_yaml("config/strategy.yaml").get("strategy", {})
    symbols = _pool_symbols()

    app_cfg = settings.app
    start_text = str(strategy_cfg.get("backtest_start_primary", "2015-01-01"))
    start_date = datetime.strptime(start_text, "%Y-%m-%d").date()

    owns_service = data_service is None
    service = data_service or DataService(provider_priority=app_cfg.data_provider_priority)
    try:
        payload = service.update_pool_daily(
            symbols=symbols,
            start_date=start_date,
            end_date=today,
            adjust=str(strategy_cfg.get("adjust", "qfq")),
            max_retries=max(int(app_cfg.daily_update_max_retries), 1),
            retry_interval_seconds=max(float(app_cfg.daily_update_retry_interval_seconds), 1.0),
        )
    finally:
        if owns_service:
            service.close()

    logger.info(
        "Daily market update finished: %s success, %s failed out of %s symbols",
        payload.get("success", 0),
        payload.get("failed", 0),
        payload.get("total", 0),
    )
    return payload
