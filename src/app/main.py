from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
import time

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routers import backtest, config, instruments, logs, overview, strategy_history, trades
from audit.app_logger import get_logger, setup_logging
from core.scheduler import SchedulerManager
from core.settings import load_settings
from data.storage.db import init_db
from engine.signal_engine import SignalEngine

settings = load_settings()
setup_logging(settings.logging.level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    Path("data").mkdir(exist_ok=True)
    init_db()
    signal_engine = SignalEngine(
        provider_priority=settings.app.data_provider_priority,
        initial_capital=settings.runtime.account_equity_default,
    )

    scheduler_manager = SchedulerManager(settings=settings)

    def poll_job() -> None:
        signal_engine.run_poll("poll_30m")

    def final_job() -> None:
        retries = max(int(settings.app.market_fetch_retry_times), 1)
        interval = max(int(settings.app.market_fetch_retry_interval_seconds), 1)

        last_payload: dict | None = None
        for attempt in range(1, retries + 1):
            payload = signal_engine.run_poll("final_1445")
            last_payload = payload
            if payload.get("status") != "data_unavailable":
                return
            if attempt < retries:
                logger.warning("final_1445 data unavailable, retry %s/%s after %ss", attempt, retries, interval)
                time.sleep(interval)

        logger.error(
            "final_1445 failed after retries, no actionable signal for today. unavailable=%s",
            (last_payload or {}).get("unavailable_symbols", []),
        )

    def update_job() -> None:
        result = signal_engine.run_daily_update()
        logger.info("Daily market data update result: %s", result.get("status", "ok"))

    scheduler_manager.start(poll_job=poll_job, final_job=final_job, update_job=update_job)

    app.state.settings = settings
    app.state.scheduler_manager = scheduler_manager
    app.state.signal_engine = signal_engine

    logger.info("Application started")
    try:
        yield
    finally:
        scheduler_manager.shutdown()
        logger.info("Application stopped")


app = FastAPI(title="Trend ETF System", version="0.1.0", lifespan=lifespan)

static_dir = Path("web/static")
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

app.include_router(overview.router)
app.include_router(config.router)
app.include_router(backtest.router)
app.include_router(strategy_history.router)
app.include_router(trades.router)
app.include_router(logs.router)
app.include_router(instruments.router)
