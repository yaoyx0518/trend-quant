from __future__ import annotations

from contextlib import asynccontextmanager
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routers import (
    backtest,
    instruments,
    logs,
    market_view,
    overview,
    rule_backtest,
    subject_market,
    trades,
)
from audit.app_logger import get_logger, setup_logging
from core.jobs import daily_market_update_job
from core.scheduler import SchedulerManager
from core.settings import load_settings
from data.storage.db import init_db

settings = load_settings()
setup_logging(settings.logging.level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    Path("data").mkdir(exist_ok=True)
    init_db()

    scheduler_manager = SchedulerManager(settings=settings)

    def update_job() -> None:
        payload = daily_market_update_job(settings)
        logger.info("Daily market data update (16:30) status: %s", payload.get("status", "ok"))

    disable_scheduler = str(os.getenv("TREND_QUANT_DISABLE_SCHEDULER", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if disable_scheduler:
        logger.warning("Scheduler disabled by TREND_QUANT_DISABLE_SCHEDULER")
    else:
        scheduler_manager.start(update_job=update_job)

    app.state.settings = settings
    app.state.scheduler_manager = scheduler_manager

    logger.info("Application started")
    try:
        yield
    finally:
        scheduler_manager.shutdown()
        logger.info("Application stopped")


app = FastAPI(title="Trend ETF System", version="0.1.0", lifespan=lifespan)

static_dir = Path("web/static")
style_file = static_dir / "style.css"
app.state.asset_version = str(int(style_file.stat().st_mtime)) if style_file.exists() else "1"


@app.middleware("http")
async def refresh_asset_version(request, call_next):
    """Ensure rendered pages reference the latest local stylesheet revision."""
    request.app.state.asset_version = (
        str(int(style_file.stat().st_mtime)) if style_file.exists() else "1"
    )
    return await call_next(request)


if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

app.include_router(overview.router)
app.include_router(backtest.router)
app.include_router(rule_backtest.router)
app.include_router(trades.router)
app.include_router(logs.router)
app.include_router(instruments.router)
app.include_router(market_view.router)
app.include_router(subject_market.router)

# ── MCP SSE endpoint (optional, requires `mcp` package) ──────────────
try:
    from trend_mcp.server import mcp as _mcp_app

    app.mount("/mcp", _mcp_app.sse_app())
    logger.info("MCP SSE endpoint mounted at /mcp/sse")
except ImportError:
    logger.info("MCP package not installed – skipping /mcp endpoint")
