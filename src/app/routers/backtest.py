from __future__ import annotations

import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Literal

import yaml
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from app.instrument_display import format_symbol_display, strip_etf_suffix
from backtest.backtest_engine import BacktestEngine
from backtest.optimization_manager import OptimizationJobManager
from core.benchmarks import (
    COMPARISON_BENCHMARKS,
    DEFAULT_BENCHMARK_SYMBOL,
)
from data.storage.db import get_db
from data.storage.runtime_store import RuntimeStore
from strategy.catalog import (
    MOMENTUM_STRATEGY_ID,
    MOMENTUM_STRATEGY_IDS,
    TREND_STRATEGY_ID,
    build_strategy_catalog,
    normalize_strategy_id,
)
from strategy.momentum_signal_modules import BUY_FILTER_REGISTRY, SELL_SIGNAL_REGISTRY, normalize_signal_modules

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/backtest", tags=["backtest"])
templates = Jinja2Templates(directory="web/templates")
store = RuntimeStore()
optimizer_job_manager = OptimizationJobManager()

_backtest_jobs: dict[str, dict] = {}
_backtest_lock = threading.Lock()


class BacktestRunRequest(BaseModel):
    start_date: str = Field(default="2025-01-01")
    end_date: str = Field(default="")
    initial_capital: float = Field(default=200000)
    strategy_id: str = Field(default=TREND_STRATEGY_ID)
    strategy_params: dict[str, float | int | str | bool | list[str]] = Field(default_factory=dict)
    selected_symbols: list[str] = Field(default_factory=list)
    benchmark_mode: Literal["equal_weight_pool", "csi500", "chinext", "symbol"] = Field(default="equal_weight_pool")
    benchmark_symbol: str = Field(default=DEFAULT_BENCHMARK_SYMBOL)
    symbol_param_overrides: dict[str, dict[str, float]] = Field(default_factory=dict)
    n_short: int = Field(default=5)
    n_mid: int = Field(default=20)
    n_long: int = Field(default=40)
    entry_threshold_min: float = Field(default=10)
    entry_threshold_max: float = Field(default=20)
    entry_threshold: float | None = Field(default=None)
    exit_threshold: float | None = Field(default=None)


class OptimizeWindow(BaseModel):
    start_date: str = Field(default="")
    end_date: str = Field(default="")


class OptimizeParamRange(BaseModel):
    key: str
    min: float
    max: float
    step: float


class OptimizeStartRequest(BaseModel):
    initial_capital: float = Field(default=200000)
    windows: list[OptimizeWindow]
    selected_params: list[OptimizeParamRange]
    enable_loo: bool = Field(default=False)
    parallel_mode: Literal["single", "auto", "manual"] = Field(default="auto")
    manual_workers: int | None = Field(default=None)


@router.get("", response_class=HTMLResponse)
async def backtest_page(request: Request) -> HTMLResponse:
    instruments_path = Path("config/instruments.yaml")
    instruments_payload = {}
    if instruments_path.exists():
        instruments_payload = yaml.safe_load(instruments_path.read_text(encoding="utf-8")) or {}
    instruments = instruments_payload.get("instruments", [])
    if not isinstance(instruments, list):
        instruments = []
    normalized_instruments: list[dict] = []
    for item in instruments:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol == "":
            continue
        name = strip_etf_suffix(str(item.get("name", "") or ""))
        copied = dict(item)
        copied["symbol"] = symbol
        copied["name"] = name
        copied["display_name"] = format_symbol_display(symbol, name)
        normalized_instruments.append(copied)
    instruments = normalized_instruments
    enabled_instrument_count = sum(1 for item in instruments if item.get("enabled", True))

    strategy_path = Path("config/strategy.yaml")
    strategy_payload = {}
    if strategy_path.exists():
        strategy_payload = yaml.safe_load(strategy_path.read_text(encoding="utf-8")) or {}
    strategy_cfg = strategy_payload.get("strategy", {})
    if not isinstance(strategy_cfg, dict):
        strategy_cfg = {}
    strategy_catalog = build_strategy_catalog(strategy_cfg)
    return templates.TemplateResponse(
        name="backtest.html",
        request=request,
        context={
            "title": "回测",
            "instruments": instruments,
            "instrument_group_label": "20行业ETF标的组",
            "enabled_instrument_count": enabled_instrument_count,
            "strategy_catalog": strategy_catalog,
            "comparison_benchmarks": [item.__dict__ for item in COMPARISON_BENCHMARKS],
        },
    )


@router.post("/api/run")
async def run_backtest(payload: BacktestRunRequest) -> dict:
    strategy_id = normalize_strategy_id(payload.strategy_id, fallback=TREND_STRATEGY_ID)

    strategy_params = payload.strategy_params if isinstance(payload.strategy_params, dict) else {}
    strategy_overrides: dict[str, float | int | str] = dict(strategy_params)

    if strategy_id == TREND_STRATEGY_ID:
        strategy_overrides.setdefault("n_short", int(payload.n_short))
        strategy_overrides.setdefault("n_mid", int(payload.n_mid))
        strategy_overrides.setdefault("n_long", int(payload.n_long))
        strategy_overrides.setdefault(
            "entry_threshold_min",
            float(payload.entry_threshold if payload.entry_threshold is not None else payload.entry_threshold_min),
        )
        strategy_overrides.setdefault("entry_threshold_max", float(payload.entry_threshold_max))
        n_short = int(strategy_overrides.get("n_short", payload.n_short))
        n_mid = int(strategy_overrides.get("n_mid", payload.n_mid))
        n_long = int(strategy_overrides.get("n_long", payload.n_long))
        entry_threshold_min = float(
            strategy_overrides.get(
                "entry_threshold_min",
                payload.entry_threshold if payload.entry_threshold is not None else payload.entry_threshold_min,
            )
        )
        entry_threshold_max = float(strategy_overrides.get("entry_threshold_max", payload.entry_threshold_max))
        if not (n_short < n_mid < n_long):
            raise HTTPException(status_code=400, detail="要求 n_short < n_mid < n_long")
        if not (entry_threshold_min > 0 and entry_threshold_max > 0):
            raise HTTPException(status_code=400, detail="要求 entry_threshold_min > 0 且 entry_threshold_max > 0")
        if entry_threshold_min > entry_threshold_max:
            raise HTTPException(status_code=400, detail="要求 entry_threshold_min <= entry_threshold_max")
    elif strategy_id in MOMENTUM_STRATEGY_IDS:
        if "buy_filters" in strategy_overrides:
            buy_filters = normalize_signal_modules(strategy_overrides.get("buy_filters"), default=[])
            unsupported_buy_filters = [x for x in buy_filters if x not in BUY_FILTER_REGISTRY]
            if unsupported_buy_filters:
                raise HTTPException(
                    status_code=400,
                    detail=f"不支持的买入过滤：{','.join(unsupported_buy_filters)}",
                )
            strategy_overrides["buy_filters"] = buy_filters

        if "sell_signals" in strategy_overrides:
            sell_signals = normalize_signal_modules(strategy_overrides.get("sell_signals"), default=[])
            unsupported_sell_signals = [x for x in sell_signals if x not in SELL_SIGNAL_REGISTRY]
            if unsupported_sell_signals:
                raise HTTPException(
                    status_code=400,
                    detail=f"不支持的卖出信号：{','.join(unsupported_sell_signals)}",
                )
            strategy_overrides["sell_signals"] = sell_signals

        n_short = int(strategy_overrides.get("n_short", 5))
        n_mid = int(strategy_overrides.get("n_mid", 20))
        n_long = int(strategy_overrides.get("n_long", 40))
        entry_threshold = float(strategy_overrides.get("entry_threshold", 10.0))
        max_holdings = int(strategy_overrides.get("max_holdings", 5))
        mom_short = int(strategy_overrides.get("momentum_window_short", 10))
        mom_long = int(strategy_overrides.get("momentum_window_long", 20))
        rebalance_weekday = int(strategy_overrides.get("rebalance_weekday", 1))
        if not (n_short < n_mid < n_long):
            raise HTTPException(status_code=400, detail="要求 n_short < n_mid < n_long")
        if not (entry_threshold > 0):
            raise HTTPException(status_code=400, detail="要求 entry_threshold > 0")
        if max_holdings <= 0:
            raise HTTPException(status_code=400, detail="要求 max_holdings > 0")
        if not (mom_short > 0 and mom_long > mom_short):
            raise HTTPException(status_code=400, detail="要求 momentum_window_long > momentum_window_short > 0")
        if not (1 <= rebalance_weekday <= 5):
            raise HTTPException(status_code=400, detail="要求 rebalance_weekday 在 [1, 5] 范围内")

    run_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
    with _backtest_lock:
        _backtest_jobs[run_id] = {
            "run_id": run_id,
            "status": "running",
            "progress_current": 0,
            "progress_total": 1,
        }

    body = payload.model_dump()

    def _progress_callback(current: int, total: int) -> None:
        with _backtest_lock:
            job = _backtest_jobs.get(run_id)
            if job:
                job["progress_current"] = current
                job["progress_total"] = total

    def _run() -> None:
        try:
            engine = BacktestEngine()
            result = engine.run(
                body,
                strategy_id=strategy_id,
                strategy_overrides=strategy_overrides,
                progress_callback=_progress_callback,
                run_id=run_id,
            )
            with _backtest_lock:
                job = _backtest_jobs.get(run_id)
                if job:
                    job["status"] = result.get("status", "ok")
                    job["progress_current"] = job.get("progress_total", 1)
                    job["result"] = result
        except Exception as exc:
            logger.exception("Backtest run_id=%s failed", run_id)
            with _backtest_lock:
                job = _backtest_jobs.get(run_id)
                if job:
                    job["status"] = "error"
                    job["error"] = str(exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return {"run_id": run_id, "status": "running"}


@router.get("/api/progress/{run_id}")
async def get_backtest_progress(run_id: str) -> dict:
    with _backtest_lock:
        job = _backtest_jobs.get(run_id)
    if job is None:
        raise HTTPException(status_code=404, detail="未找到运行 ID")
    resp: dict = {
        "run_id": job["run_id"],
        "status": job["status"],
        "progress_current": job.get("progress_current", 0),
        "progress_total": job.get("progress_total", 1),
        "error": job.get("error"),
    }
    if job["status"] not in ("running",) and job.get("result"):
        resp["result"] = job["result"]
    return resp


@router.get("/api/list")
async def list_backtests(limit: int = 40, favorite_only: bool = False) -> dict:
    items = get_db().list_backtests_summary(limit=limit)
    if favorite_only:
        items = [item for item in items if item.get("is_favorite")]
    return {"items": items}


class FavoriteToggle(BaseModel):
    is_favorite: bool


@router.post("/api/{run_id}/favorite")
async def toggle_favorite(run_id: str, payload: FavoriteToggle) -> dict:
    ok = get_db().set_backtest_favorite(run_id, payload.is_favorite)
    if not ok:
        raise HTTPException(status_code=404, detail="未找到运行 ID")
    return {"run_id": run_id, "is_favorite": payload.is_favorite}


@router.delete("/api/{run_id}")
async def delete_backtest(run_id: str) -> dict:
    ok = get_db().delete_backtest(run_id)
    if not ok:
        raise HTTPException(status_code=404, detail="未找到运行 ID")
    with _backtest_lock:
        _backtest_jobs.pop(run_id, None)
    return {"run_id": run_id, "deleted": True}


@router.get("/api/optimize/params")
async def get_optimize_params() -> dict:
    return optimizer_job_manager.discover_tunable_params()


@router.post("/api/optimize/start")
async def start_optimize(payload: OptimizeStartRequest) -> dict:
    body = payload.model_dump()
    return optimizer_job_manager.start_job(body)


@router.get("/api/optimize/{job_id}/status")
async def get_optimize_status(job_id: str) -> dict:
    status_payload = optimizer_job_manager.get_status(job_id)
    if status_payload is None:
        raise HTTPException(status_code=404, detail="未找到任务 ID")
    return status_payload


@router.post("/api/optimize/{job_id}/cancel")
async def cancel_optimize(job_id: str) -> dict:
    result = optimizer_job_manager.cancel_job(job_id)
    if result.get("status") == "not_found":
        raise HTTPException(status_code=404, detail="未找到任务 ID")
    return result


@router.get("/api/optimize/{job_id}/result")
async def get_optimize_result(job_id: str) -> dict:
    result_payload = optimizer_job_manager.get_result(job_id)
    if result_payload is None:
        raise HTTPException(status_code=404, detail="未找到任务 ID")
    return result_payload


@router.get("/api/{run_id}")
async def get_backtest_result(run_id: str) -> dict:
    with _backtest_lock:
        job = _backtest_jobs.get(run_id)
        if job and job.get("result"):
            return job["result"]
    result = get_db().get_backtest(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="未找到运行 ID")
    return result
