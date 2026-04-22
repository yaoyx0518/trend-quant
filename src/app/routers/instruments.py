from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import yaml
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from data.service import DataService
from data.storage.db import get_db
from data.storage.market_store import MarketStore
from data.storage.runtime_store import RuntimeStore

router = APIRouter(prefix="/instruments", tags=["instruments"])
templates = Jinja2Templates(directory="web/templates")
market_store = MarketStore()
runtime_store = RuntimeStore()


class InstrumentBackfillRequest(BaseModel):
    start_date: str = Field(default="")
    end_date: str = Field(default="")
    adjust: str = Field(default="")


def _to_date(text: str, fallback: date) -> date:
    raw = str(text or "").strip()
    if raw == "":
        return fallback
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _load_yaml(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def _symbol_to_code(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    if "." in text:
        return text.split(".", 1)[0]
    return text


def _symbol_suffix(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    if "." in text:
        return text.split(".", 1)[1]
    return ""


def _normalize_symbol(raw_symbol: str) -> str:
    text = str(raw_symbol or "").strip().upper()
    if text == "":
        return ""
    if "." in text:
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return text
    suffix = ".SS" if digits.startswith(("5", "6")) else ".SZ"
    return f"{digits}{suffix}"


def _date_span(df: pd.DataFrame) -> tuple[str | None, str | None]:
    if df.empty or "time" not in df.columns:
        return None, None
    series = pd.to_datetime(df["time"], errors="coerce").dropna()
    if series.empty:
        return None, None
    return series.min().date().isoformat(), series.max().date().isoformat()


def _config_name_map() -> dict[str, str]:
    payload = _load_yaml("config/instruments.yaml")
    instruments = payload.get("instruments", []) if isinstance(payload, dict) else []
    if not isinstance(instruments, list):
        return {}

    out: dict[str, str] = {}
    for item in instruments:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol == "":
            continue
        out[symbol] = str(item.get("name", "") or "").strip()
    return out


@router.get("", response_class=HTMLResponse)
async def instruments_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        name="instruments.html",
        request=request,
        context={"title": "Instruments"},
    )


@router.get("/api/list")
async def list_instruments() -> dict:
    config_payload = _load_yaml("config/instruments.yaml")
    config_items = config_payload.get("instruments", []) if isinstance(config_payload, dict) else []
    if not isinstance(config_items, list):
        config_items = []

    config_by_symbol: dict[str, dict] = {}
    for item in config_items:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol == "":
            continue
        config_by_symbol[symbol] = item

    name_map = _config_name_map()

    known_symbols = set(config_by_symbol.keys())
    for symbol in get_db().list_market_symbols():
        known_symbols.add(symbol.upper())

    items: list[dict] = []
    for symbol in sorted(known_symbols):
        summary = get_db().get_market_data_summary(symbol)
        rows = summary.get("rows", 0)
        local_start = summary.get("start")
        local_end = summary.get("end")

        cfg = config_by_symbol.get(symbol, {})
        in_config = bool(cfg)
        enabled = bool(cfg.get("enabled", False)) if in_config else False
        name = str(name_map.get(symbol, "") or "")
        items.append(
            {
                "symbol": symbol,
                "code": _symbol_to_code(symbol),
                "exchange": _symbol_suffix(symbol),
                "name": name,
                "enabled": enabled,
                "in_config": in_config,
                "rows": rows,
                "local_start_date": local_start,
                "local_end_date": local_end,
                "path": str(path),
            }
        )

    items.sort(key=lambda x: (not bool(x.get("enabled")), str(x.get("symbol", ""))))
    return {"items": items, "count": len(items), "as_of": datetime.now().isoformat()}


@router.post("/api/{symbol}/backfill")
async def backfill_instrument(symbol: str, payload: InstrumentBackfillRequest, request: Request) -> dict:
    normalized_symbol = _normalize_symbol(symbol)
    if normalized_symbol == "":
        raise HTTPException(status_code=400, detail="invalid symbol")

    if str(payload.start_date or "").strip() == "":
        raise HTTPException(status_code=400, detail="start_date is required")

    try:
        start_date = _to_date(payload.start_date, date(2020, 1, 1))
        end_date = _to_date(payload.end_date, date.today())
    except ValueError:
        raise HTTPException(status_code=400, detail="date format must be YYYY-MM-DD")

    strategy_payload = _load_yaml("config/strategy.yaml")
    strategy_cfg = strategy_payload.get("strategy", {}) if isinstance(strategy_payload, dict) else {}
    adjust_default = str(strategy_cfg.get("adjust", "qfq")) if isinstance(strategy_cfg, dict) else "qfq"
    adjust = str(payload.adjust or adjust_default).strip().lower() or "qfq"

    provider_priority = None
    if hasattr(request.app.state, "settings"):
        provider_priority = list(getattr(request.app.state.settings.app, "data_provider_priority", []) or [])
    data_service = DataService(provider_priority=provider_priority)
    result = data_service.backfill_daily_history(
        symbol=normalized_symbol,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )
    result["adjust"] = adjust
    result["requested_symbol"] = symbol
    result["symbol"] = normalized_symbol
    result["request_adjusted_to_earliest_available"] = bool(
        result.get("fetched_start")
        and result.get("requested_start")
        and str(result.get("fetched_start")) > str(result.get("requested_start"))
    )

    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    runtime_store.write_json(f"advice/instrument_backfill_{normalized_symbol}_{stamp}.json", result)
    return {"ok": True, "result": result}
