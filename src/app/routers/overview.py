from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.instrument_display import build_symbol_display, load_instrument_name_map
from core.settings import load_settings
from data.storage.db import get_db
from data.storage.runtime_store import RuntimeStore
from portfolio.service import PortfolioService

router = APIRouter(tags=["overview"])
templates = Jinja2Templates(directory="web/templates")
store = RuntimeStore()
portfolio_service = PortfolioService(runtime_store=store)


def _normalize_enum_text(value: object) -> str:
    text = str(value)
    if text.startswith("SignalAction.") or text.startswith("SignalLevel."):
        return text.split(".", 1)[1]
    return text


def _latest_signal_payload() -> dict:
    latest = get_db().get_latest_signals(limit=1)
    return latest[0] if latest else {}


def _recent_backtests(limit: int = 40) -> list[dict]:
    return get_db().list_backtests(limit=limit)


@router.get("/", response_class=HTMLResponse)
async def overview_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        name="overview.html",
        request=request,
        context={
            "title": "System Overview",
        },
    )


@router.get("/api/overview")
async def overview_api(request: Request) -> dict:
    settings = getattr(request.app.state, "settings", None) or load_settings()
    name_map = load_instrument_name_map()

    today = date.today()
    snapshot = portfolio_service.build_snapshot(today, settings.runtime.account_equity_default)
    latest_signal = _latest_signal_payload()

    signals = latest_signal.get("signals", []) if isinstance(latest_signal, dict) else []
    action_signals: list[dict] = []
    price_map: dict[str, float] = {}

    for sig in signals:
        symbol = str(sig.get("symbol", ""))
        action = _normalize_enum_text(sig.get("action", "HOLD"))
        level = _normalize_enum_text(sig.get("level", "INFO"))
        calc = sig.get("calc_details", {}) if isinstance(sig.get("calc_details"), dict) else {}
        price = float(calc.get("price", 0.0) or 0.0)
        if symbol and price > 0:
            price_map[symbol] = price

        if action != "HOLD" or level == "ACTION":
            action_signals.append(
                {
                    "symbol": symbol,
                    "symbol_display": build_symbol_display(symbol, name_map),
                    "action": action,
                    "level": level,
                    "trend_score": sig.get("trend_score"),
                    "reason": sig.get("reason"),
                    "suggested_qty": sig.get("suggested_qty", 0),
                }
            )

    equity = portfolio_service.estimate_equity(snapshot, price_map)
    positions = snapshot.get("positions", {}) if isinstance(snapshot, dict) else {}
    position_rows = []
    for symbol, pos in positions.items():
        position_rows.append(
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
        "app": settings.app.name,
        "status": "ok",
        "latest_signal": {
            "ts": latest_signal.get("ts"),
            "trigger": latest_signal.get("trigger"),
            "status": latest_signal.get("status"),
            "action_count": len(action_signals),
        },
        "portfolio": {
            "as_of_date": snapshot.get("as_of_date"),
            "cash": snapshot.get("cash"),
            "equity": equity,
            "position_count": len(position_rows),
            "trade_count": snapshot.get("trade_count", 0),
            "positions": position_rows,
        },
        "action_signals": action_signals,
        "recent_backtests": _recent_backtests(limit=40),
    }
