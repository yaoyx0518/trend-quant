from __future__ import annotations

from pathlib import Path

import yaml
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

router = APIRouter(prefix="/config", tags=["config"])
templates = Jinja2Templates(directory="web/templates")


class RawConfigUpdate(BaseModel):
    app: dict | None = None
    strategy: dict | None = None
    instruments: list[dict] | None = None


@router.get("", response_class=HTMLResponse)
async def config_page(request: Request) -> HTMLResponse:
    app_cfg = Path("config/app.yaml").read_text(encoding="utf-8")
    strategy_cfg = Path("config/strategy.yaml").read_text(encoding="utf-8")
    instruments_cfg = Path("config/instruments.yaml").read_text(encoding="utf-8")
    return templates.TemplateResponse(
        name="config.html",
        request=request,
        context={
            "title": "Config",
            "app_cfg": app_cfg,
            "strategy_cfg": strategy_cfg,
            "instruments_cfg": instruments_cfg,
        },
    )


@router.get("/api/raw")
async def read_config_raw() -> dict:
    return {
        "app": yaml.safe_load(Path("config/app.yaml").read_text(encoding="utf-8")),
        "strategy": yaml.safe_load(Path("config/strategy.yaml").read_text(encoding="utf-8")),
        "instruments": yaml.safe_load(Path("config/instruments.yaml").read_text(encoding="utf-8")),
    }


@router.post("/api/raw")
async def update_config_raw(payload: RawConfigUpdate) -> dict:
    if payload.app is not None:
        Path("config/app.yaml").write_text(yaml.safe_dump(payload.app, sort_keys=False, allow_unicode=True), encoding="utf-8")
    if payload.strategy is not None:
        Path("config/strategy.yaml").write_text(yaml.safe_dump(payload.strategy, sort_keys=False, allow_unicode=True), encoding="utf-8")
    if payload.instruments is not None:
        Path("config/instruments.yaml").write_text(
            yaml.safe_dump({"instruments": payload.instruments}, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
    return {"ok": True, "message": "Config saved. Changes apply immediately."}
