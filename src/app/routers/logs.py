from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(prefix="/logs", tags=["logs"])
templates = Jinja2Templates(directory="web/templates")


@router.get("", response_class=HTMLResponse)
async def logs_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(name="logs.html", request=request, context={"title": "Logs"})


@router.get("/api/calc")
async def calc_logs(limit: int = 100) -> dict:
    file_path = Path("logs/calc/calc.jsonl")
    if not file_path.exists():
        return {"items": []}

    lines = file_path.read_text(encoding="utf-8").splitlines()
    tail = lines[-max(limit, 1):]
    return {"items": tail}
