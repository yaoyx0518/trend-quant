from __future__ import annotations

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

router = APIRouter(prefix="/backtest", tags=["backtest"])
templates = Jinja2Templates(directory="web/templates")
store = RuntimeStore()
optimizer_job_manager = OptimizationJobManager()
DEFAULT_BENCHMARK_SYMBOL = "512500.SS"


class BacktestRunRequest(BaseModel):
    start_date: str = Field(default="2025-01-01")
    end_date: str = Field(default="")
    initial_capital: float = Field(default=200000)
    strategy_id: str = Field(default=TREND_STRATEGY_ID)
    strategy_params: dict[str, float | int | str | bool | list[str]] = Field(default_factory=dict)
    selected_symbols: list[str] = Field(default_factory=list)
    benchmark_mode: Literal["equal_weight_pool", "symbol"] = Field(default="equal_weight_pool")
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
            "title": "Backtest",
            "instruments": instruments,
            "strategy_catalog": strategy_catalog,
        },
    )


@router.post("/api/run")
async def run_backtest(payload: BacktestRunRequest) -> dict:
    strategy_id = normalize_strategy_id(payload.strategy_id, fallback=TREND_STRATEGY_ID)
    benchmark_mode = str(payload.benchmark_mode or "equal_weight_pool").strip().lower()
    benchmark_symbol = str(payload.benchmark_symbol or "").strip().upper()
    if benchmark_mode == "symbol" and benchmark_symbol == "":
        benchmark_symbol = DEFAULT_BENCHMARK_SYMBOL

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
            raise HTTPException(status_code=400, detail="require n_short < n_mid < n_long")
        if not (entry_threshold_min > 0 and entry_threshold_max > 0):
            raise HTTPException(status_code=400, detail="require entry_threshold_min > 0 and entry_threshold_max > 0")
        if entry_threshold_min > entry_threshold_max:
            raise HTTPException(status_code=400, detail="require entry_threshold_min <= entry_threshold_max")
    elif strategy_id in MOMENTUM_STRATEGY_IDS:
        if "buy_filters" in strategy_overrides:
            buy_filters = normalize_signal_modules(strategy_overrides.get("buy_filters"), default=[])
            unsupported_buy_filters = [x for x in buy_filters if x not in BUY_FILTER_REGISTRY]
            if unsupported_buy_filters:
                raise HTTPException(
                    status_code=400,
                    detail=f"unsupported buy_filters: {','.join(unsupported_buy_filters)}",
                )
            strategy_overrides["buy_filters"] = buy_filters

        if "sell_signals" in strategy_overrides:
            sell_signals = normalize_signal_modules(strategy_overrides.get("sell_signals"), default=[])
            unsupported_sell_signals = [x for x in sell_signals if x not in SELL_SIGNAL_REGISTRY]
            if unsupported_sell_signals:
                raise HTTPException(
                    status_code=400,
                    detail=f"unsupported sell_signals: {','.join(unsupported_sell_signals)}",
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
            raise HTTPException(status_code=400, detail="require n_short < n_mid < n_long")
        if not (entry_threshold > 0):
            raise HTTPException(status_code=400, detail="require entry_threshold > 0")
        if max_holdings <= 0:
            raise HTTPException(status_code=400, detail="require max_holdings > 0")
        if not (mom_short > 0 and mom_long > mom_short):
            raise HTTPException(status_code=400, detail="require momentum_window_long > momentum_window_short > 0")
        if not (1 <= rebalance_weekday <= 5):
            raise HTTPException(status_code=400, detail="require rebalance_weekday in [1, 5]")

    engine = BacktestEngine()
    body = payload.model_dump()
    body["benchmark_mode"] = benchmark_mode
    body["benchmark_symbol"] = benchmark_symbol
    try:
        return engine.run(
            body,
            strategy_id=strategy_id,
            strategy_overrides=strategy_overrides,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/api/list")
async def list_backtests(limit: int = 40) -> dict:
    items = get_db().list_backtests_summary(limit=limit)
    return {"items": items}





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
        raise HTTPException(status_code=404, detail="job_id not found")
    return status_payload


@router.post("/api/optimize/{job_id}/cancel")
async def cancel_optimize(job_id: str) -> dict:
    result = optimizer_job_manager.cancel_job(job_id)
    if result.get("status") == "not_found":
        raise HTTPException(status_code=404, detail="job_id not found")
    return result


@router.get("/api/optimize/{job_id}/result")
async def get_optimize_result(job_id: str) -> dict:
    result_payload = optimizer_job_manager.get_result(job_id)
    if result_payload is None:
        raise HTTPException(status_code=404, detail="job_id not found")
    return result_payload
@router.get("/api/{run_id}")
async def get_backtest_result(run_id: str) -> dict:
    result = get_db().get_backtest(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="run_id not found")
    return result

