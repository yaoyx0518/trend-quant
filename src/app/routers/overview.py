from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.instrument_display import build_symbol_display, load_instrument_name_map
from core.settings import load_settings
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
    signal_dir = Path(store.base_dir) / "signals"
    if not signal_dir.exists():
        return {}

    files = [p for p in signal_dir.glob("*.json") if p.name != "latest_state.json"]
    if not files:
        return {}

    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    target = files[0]
    return store.read_json(str(Path("signals") / target.name), default={}) or {}


def _recent_backtests(limit: int = 40) -> list[dict]:
    base = Path(store.base_dir) / "backtests"
    if not base.exists():
        return []

    items: list[dict] = []
    for p in sorted(base.glob("*/result.json"), key=lambda x: x.stat().st_mtime, reverse=True):
        run_id = p.parent.name
        data = store.read_json(str(Path("backtests") / run_id / "result.json"), default={}) or {}
        status = str(data.get("status", ""))
        summary = data.get("summary", {}) if isinstance(data.get("summary"), dict) else {}
        input_payload = data.get("input", {}) if isinstance(data.get("input"), dict) else {}
        meta_payload = data.get("meta", {}) if isinstance(data.get("meta"), dict) else {}
        strategy_overrides = input_payload.get("strategy_overrides")
        strategy_params = input_payload.get("strategy_params")
        params = strategy_overrides if isinstance(strategy_overrides, dict) else (
            strategy_params if isinstance(strategy_params, dict) else {}
        )
        items.append(
            {
                "run_id": run_id,
                "strategy": meta_payload.get("strategy_id") or input_payload.get("strategy_id"),
                "start_date": input_payload.get("start_date"),
                "end_date": input_payload.get("end_date"),
                "params": params,
                "total_return": summary.get("total_return") if status == "ok" else None,
                "win_rate": summary.get("win_rate") if status == "ok" else None,
                "profit_factor": summary.get("profit_factor") if status == "ok" else None,
                "sharpe": summary.get("sharpe") if status == "ok" else None,
                "trade_count": summary.get("trade_count") if status == "ok" else None,
                "timeline_days": meta_payload.get("timeline_days"),
            }
        )
        if len(items) >= max(limit, 1):
            break
    return items


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
