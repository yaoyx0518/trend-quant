from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from audit.calc_logger import list_calc_runs, list_calc_symbols, query_calc_records

router = APIRouter(prefix="/logs", tags=["logs"])
templates = Jinja2Templates(directory="web/templates")


@router.get("", response_class=HTMLResponse)
async def logs_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(name="logs.html", request=request, context={"title": "日志"})


@router.get("/api/calc")
async def calc_logs(
    limit: int = 200,
    symbol: str = "",
    start: str = "",
    end: str = "",
    source: str = "",
    run_id: str = "",
    raw: bool = False,
) -> dict:
    file_path = Path("logs/calc/calc.jsonl")
    if not file_path.exists():
        return {"items": [], "symbols": [], "meta": {"total": 0, "returned": 0}}

    if raw:
        lines = file_path.read_text(encoding="utf-8").splitlines()
        tail = lines[-max(limit, 1):] if limit != 0 else lines
        return {"items": tail}

    return query_calc_records(
        symbol=symbol,
        start=start,
        end=end,
        source=source,
        run_id=run_id,
        limit=limit,
    )


@router.get("/api/calc/symbols")
async def calc_log_symbols() -> dict:
    return {"items": list_calc_symbols()}


@router.get("/api/calc/runs")
async def calc_log_runs(source: str = "") -> dict:
    return {"items": list_calc_runs(source=source)}


@router.get("/api/calc/{line_no}")
async def calc_log_detail(line_no: int) -> dict:
    file_path = Path("logs/calc/calc.jsonl")
    if not file_path.exists() or line_no <= 0:
        return {"item": None}
    with file_path.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f, 1):
            if idx != line_no:
                continue
            try:
                return {"item": json.loads(line)}
            except json.JSONDecodeError:
                return {"item": None}
    return {"item": None}
