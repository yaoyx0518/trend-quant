from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from app.instrument_display import build_symbol_display, load_instrument_name_map
from core.settings import load_settings
from data.storage.db import get_db
from data.storage.runtime_store import RuntimeStore
from portfolio.service import PortfolioService

router = APIRouter(prefix="/trades", tags=["trades"])
templates = Jinja2Templates(directory="web/templates")
store = RuntimeStore()
portfolio_service = PortfolioService(runtime_store=store)


class ManualTradePayload(BaseModel):
    trade_date: str
    symbol: str
    side: str
    qty: int = Field(gt=0)
    price: float = Field(gt=0)
    fee: float = Field(ge=0)
    trade_time: str
    note: str = ""

@router.get("", response_class=HTMLResponse)
async def trades_page(request: Request) -> HTMLResponse:
    today = date.today().isoformat()
    items = get_db().get_trades_by_date(today)
    return templates.TemplateResponse(
        name="trades.html",
        request=request,
        context={"title": "Manual Trades", "items": items, "trade_date": today},
    )


@router.get("/api/manual")
async def get_manual_trades(trade_date: str) -> dict:
    items = get_db().get_trades_by_date(trade_date)

    name_map = load_instrument_name_map()
    normalized_items: list[dict] = []
    for row in items:
        if not isinstance(row, dict):
            continue
        copied = dict(row)
        symbol = str(copied.get("symbol", "")).strip().upper()
        copied["symbol"] = symbol
        copied["symbol_display"] = build_symbol_display(symbol, name_map)
        normalized_items.append(copied)
    return {"items": normalized_items}


@router.post("/api/manual")
async def save_manual_trade(payload: ManualTradePayload) -> dict:
    side = payload.side.strip().upper()
    if side not in {"BUY", "SELL"}:
        raise HTTPException(status_code=400, detail="side must be BUY or SELL")

    row = payload.model_dump()
    row["side"] = side
    row["symbol"] = payload.symbol.strip().upper()

    db = get_db()
    db.add_trade(row)
    count = len(db.get_trades_by_date(payload.trade_date))
    return {"ok": True, "count": count}


@router.get("/api/portfolio")
async def get_portfolio_snapshot(as_of_date: Optional[str] = None, initial_capital: Optional[float] = None) -> dict:
    settings = load_settings()
    name_map = load_instrument_name_map()
    target_date = date.fromisoformat(as_of_date) if as_of_date else date.today()
    capital = float(initial_capital) if initial_capital is not None else float(settings.runtime.account_equity_default)

    snapshot = portfolio_service.build_snapshot(as_of_date=target_date, initial_capital=capital)
    positions = snapshot.get("positions", {})

    rows = []
    for symbol, pos in positions.items():
        rows.append(
            {
                "symbol": symbol,
                "symbol_display": build_symbol_display(symbol, name_map),
                "qty": int(pos.get("qty", 0)),
                "sellable_qty": int(pos.get("sellable_qty", 0)),
                "avg_price": float(pos.get("avg_price", 0.0) or 0.0),
                "buy_date": pos.get("buy_date"),
            }
        )

    return {
        "as_of_date": snapshot.get("as_of_date"),
        "cash": snapshot.get("cash"),
        "trade_count": snapshot.get("trade_count", 0),
        "position_count": len(rows),
        "positions": rows,
    }
