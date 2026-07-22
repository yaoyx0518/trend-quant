from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from services.manual_trade import compute_manual_trade
from services.stop_loss import StopLossError

router = APIRouter(prefix="/manual-trade", tags=["manual-trade"])
templates = Jinja2Templates(directory="web/templates")


class ManualTradeEvaluateRequest(BaseModel):
    symbol: str = Field(..., min_length=1, description="标的代码，如 510300 或 510300.SS")
    buy_date: date = Field(..., description="买入日期")
    buy_price: float = Field(..., gt=0, description="买入均价")


@router.get("", response_class=HTMLResponse)
async def manual_trade_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        name="manual_trade.html", request=request, context={"title": "手工交易"}
    )


@router.post("/api/evaluate")
async def evaluate_manual_trade(payload: ManualTradeEvaluateRequest) -> dict:
    try:
        return compute_manual_trade(
            payload.symbol, payload.buy_date.isoformat(), payload.buy_price
        )
    except StopLossError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
