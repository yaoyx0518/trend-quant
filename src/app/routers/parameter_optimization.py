from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(prefix="/parameter-optimization", tags=["parameter-optimization"])
templates = Jinja2Templates(directory="web/templates")


@router.get("", response_class=HTMLResponse)
async def parameter_optimization_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        name="parameter_optimization.html",
        request=request,
        context={"title": "参数优化"},
    )
