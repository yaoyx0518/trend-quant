from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from core.calendar import previous_trading_day
from rule_backtest.service import RuleBacktestService

router = APIRouter(prefix="/rule-backtest", tags=["rule-backtest"])
templates = Jinja2Templates(directory="web/templates")
service = RuleBacktestService()


class RuleBacktestRunRequest(BaseModel):
    strategy_id: str
    symbol: str
    start_date: str = Field(default="")
    end_date: str = Field(default="")
    initial_capital: float = Field(default=1_000_000)
    slippage: float = Field(default=0.002)
    fee_rate: float = Field(default=0.0000854)
    fee_min: float = Field(default=5.0)
    lot_size: int = Field(default=100)
    instrument_type: str = Field(default="")
    stock_stamp_tax_rate: float = Field(default=0.001)
    debug_log_enabled: bool | None = Field(default=None)


class RuleStrategySaveRequest(BaseModel):
    strategy: dict
    overwrite: bool = Field(default=False)


def _cap_end_date(end_date_str: str) -> str:
    """Ensure backtest *end_date* does not include today or future dates.

    Intraday data is never persisted, so backtesting must stop at the
    most recent confirmed trading day.
    """
    if not end_date_str.strip():
        return end_date_str
    try:
        requested = date.fromisoformat(end_date_str.strip())
    except (ValueError, TypeError):
        return end_date_str
    yesterday = previous_trading_day(date.today())
    if requested >= date.today():
        return yesterday.isoformat()
    return end_date_str


@router.get("", response_class=HTMLResponse)
async def rule_backtest_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        name="rule_backtest.html",
        request=request,
        context={"title": "策略管理"},
    )


@router.get("/api/meta")
async def get_rule_backtest_meta() -> dict:
    return {
        "strategies": service.list_strategies(),
        "instruments": service.list_instruments(),
        "indicators": service.list_indicators(),
    }


@router.post("/api/run")
async def run_rule_backtest(payload: RuleBacktestRunRequest) -> dict:
    # Cap end_date to previous trading day (intraday data is never persisted).
    payload.end_date = _cap_end_date(payload.end_date)
    try:
        return service.run(payload.model_dump())
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/strategies")
async def save_rule_strategy(payload: RuleStrategySaveRequest) -> dict:
    try:
        return service.save_strategy(payload.strategy, overwrite=payload.overwrite)
    except FileExistsError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
