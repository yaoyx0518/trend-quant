from __future__ import annotations

import threading
import time as _time
import uuid
from collections import defaultdict
from datetime import datetime
from math import isfinite
from typing import Any

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.routers.market_view import _trend_config, compute_trend_indicator
from core.calendar import is_realtime_available, is_trading_day, previous_trading_day, trading_session_status
from data.intraday_service import build_intraday_dashboard, _detect_trend_phase
from data.service import DataService
from data.storage.db import get_db

router = APIRouter(prefix="/subject-market", tags=["subject-market"])
templates = Jinja2Templates(directory="web/templates")

DISPLAY_DAYS = 61
SOURCE_HISTORY_DAYS = 90
_dashboard_cache: tuple[tuple[str, int, str], dict] | None = None


def _number(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def _ma5(values: list[float | None]) -> list[float | None]:
    series = pd.Series(values, dtype="float64").rolling(5, min_periods=5).mean()
    return [_number(value) for value in series]


def _strength(values: list[float], value: float | None) -> int | None:
    if value is None or not values:
        return None
    return round(sum(score <= value for score in values) * 100 / len(values))


def _priority(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 999999


def _key_tuple(key: object) -> tuple[object, ...]:
    return key if isinstance(key, tuple) else (key,)


def _aggregate_daily(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    metrics = {
        "trend_score": "trend_score",
        "daily_change_pct": "return_1d",
        "change_5d": "return_5d",
        "change_20d": "return_20d",
        "change_60d": "return_60d",
        "close": "close",
    }
    columns = [*group_columns, "time", "amount", *metrics.values()]
    work = frame[columns].copy()
    amount = pd.to_numeric(work["amount"], errors="coerce").to_numpy(dtype=float)
    valid_amount = np.isfinite(amount) & (amount > 0)
    work["_amount_total"] = np.where(np.isfinite(amount) & (amount >= 0), amount, 0.0)
    aggregation_columns = ["_amount_total"]
    for target, source in metrics.items():
        values = pd.to_numeric(work[source], errors="coerce").to_numpy(dtype=float)
        valid = valid_amount & np.isfinite(values)
        numerator = f"_{target}_numerator"
        denominator = f"_{target}_denominator"
        work[numerator] = np.where(valid, values * amount, 0.0)
        work[denominator] = np.where(valid, amount, 0.0)
        aggregation_columns.extend([numerator, denominator])
    daily = work.groupby([*group_columns, "time"], as_index=False, sort=True)[aggregation_columns].sum()
    daily["amount"] = daily.pop("_amount_total").where(lambda values: values > 0, np.nan)
    for target in metrics:
        numerator = daily.pop(f"_{target}_numerator")
        denominator = daily.pop(f"_{target}_denominator")
        daily[target] = (numerator / denominator.replace(0.0, np.nan)).astype("float64")
    return daily


def _metrics_summary(daily: pd.DataFrame, metadata: dict) -> dict | None:
    if daily.empty:
        return None
    daily = daily.sort_values("time")
    raw_trend = [_number(value) for value in daily["trend_score"]]
    trend_ma5 = _ma5(raw_trend)
    raw_close = [_number(value) for value in daily["close"]] if "close" in daily.columns else []
    dates = [pd.Timestamp(value).date().isoformat() for value in daily["time"]]
    recent = daily.tail(DISPLAY_DAYS)
    latest = daily.iloc[-1]

    # Detect trend phase signal.
    phase_info = _detect_trend_phase(raw_trend, trend_ma5, raw_close, dates)

    return {
        "member_count": int(metadata["member_count"]),
        "trend_score": _number(latest["trend_score"]),
        "trend_ma5": trend_ma5[-1] if trend_ma5 else None,
        "daily_change_pct": _number(latest["daily_change_pct"]),
        "change_5d": _number(latest["change_5d"]),
        "change_20d": _number(latest["change_20d"]),
        "change_60d": _number(latest["change_60d"]),
        "amount": _number(latest["amount"]),
        "trend_history": trend_ma5[-DISPLAY_DAYS:],
        "trend_dates": [pd.Timestamp(value).date().isoformat() for value in recent["time"]],
        "as_of": pd.Timestamp(latest["time"]).date().isoformat(),
        "priority_l1": _priority(metadata["priority_l1"]),
        "priority_l2": _priority(metadata["priority_l2"]),
        "priority_l3": _priority(metadata["priority_l3"]),
        "trend_phase": phase_info["phase"],
        "trend_phase_days": phase_info["days"],
        "trend_phase_change_pct": phase_info["change_pct"],
        "trend_phase_signal_date": phase_info["signal_date"],
    }


def _build_level_summaries(calculated: pd.DataFrame, group_columns: list[str]) -> list[dict]:
    metadata_frame = (
        calculated.groupby(group_columns, as_index=False, sort=False)
        .agg(
            member_count=("symbol", "nunique"),
            priority_l1=("priority_l1", "min"),
            priority_l2=("priority_l2", "min"),
            priority_l3=("priority_l3", "min"),
        )
    )
    metadata_by_key = {
        tuple(str(row._asdict()[column]) for column in group_columns): row._asdict()
        for row in metadata_frame.itertuples(index=False)
    }
    daily = _aggregate_daily(calculated, group_columns)
    summaries: list[dict] = []
    for raw_key, level_daily in daily.groupby(group_columns, sort=False):
        key = tuple(str(value) for value in _key_tuple(raw_key))
        summary = _metrics_summary(level_daily, metadata_by_key[key])
        if summary is not None:
            summary.update(dict(zip(group_columns, key)))
            summaries.append(summary)
    return summaries


def _assign_strength(items: list[dict], scope_columns: tuple[str, ...]) -> None:
    values_by_scope: dict[tuple[str, ...], list[float]] = defaultdict(list)
    for item in items:
        value = _number(item.get("trend_ma5"))
        if value is not None:
            values_by_scope[tuple(str(item[column]) for column in scope_columns)].append(value)
    for item in items:
        scope = tuple(str(item[column]) for column in scope_columns)
        item["strength"] = _strength(values_by_scope[scope], _number(item.get("trend_ma5")))


def _assign_envelope(item: dict, components: list[dict]) -> None:
    """Attach MA5 extrema of ``components`` to ``item``, aligned by trading date."""
    values_by_date: dict[str, list[float]] = defaultdict(list)
    for component in components:
        for date, value in zip(component.get("trend_dates", []), component.get("trend_history", [])):
            number = _number(value)
            if number is not None:
                values_by_date[str(date)].append(number)
    upper: list[float | None] = []
    lower: list[float | None] = []
    for date in item.get("trend_dates", []):
        values = values_by_date.get(str(date), [])
        upper.append(max(values) if values else None)
        lower.append(min(values) if values else None)
    item["trend_upper_history"] = upper
    item["trend_lower_history"] = lower


def _sort_items(items: list[dict], name_key: str) -> None:
    items.sort(
        key=lambda item: (
            item["strength"] is None,
            -(item["strength"] or 0),
            item["priority_l3"],
            str(item.get(name_key) or ""),
        )
    )


def build_subject_dashboard_payload(db=None) -> dict:
    db = db or get_db()
    rows = db.load_market_dashboard_history(days=SOURCE_HISTORY_DAYS)
    if not rows:
        return {
            "as_of": None,
            "groups": [],
            "secondary_count": 0,
            "category_count": 0,
            "instrument_count": 0,
        }

    source = pd.DataFrame(rows)
    if "name" not in source.columns:
        source["name"] = source["symbol"]
    source["name"] = source["name"].fillna(source["symbol"]).astype(str)
    source["time"] = pd.to_datetime(source["time"], errors="coerce")
    source = source.dropna(subset=["time", "open", "high", "low", "close"]).copy()
    for column in ("open", "high", "low", "close", "volume", "amount"):
        source[column] = pd.to_numeric(source[column], errors="coerce")

    instrument_frames: list[pd.DataFrame] = []
    trend_config = _trend_config()
    for _, history in source.groupby("symbol", sort=False):
        data = history.sort_values("time").reset_index(drop=True).copy()
        data["trend_score"] = compute_trend_indicator(data, trend_config).get("score", [])
        for period in (1, 5, 20, 60):
            data[f"return_{period}d"] = data["close"].pct_change(periods=period) * 100.0
        instrument_frames.append(data)
    calculated = pd.concat(instrument_frames, ignore_index=True)

    l2_columns = ["category_l1", "category_l2"]
    l3_columns = [*l2_columns, "category_l3"]
    instrument_columns = [*l3_columns, "symbol", "name"]
    l2_items = _build_level_summaries(calculated, l2_columns)
    l3_items = _build_level_summaries(calculated, l3_columns)
    instruments = _build_level_summaries(calculated, instrument_columns)
    _assign_strength(l2_items, ("category_l1",))
    _assign_strength(l3_items, ("category_l1",))
    _assign_strength(instruments, ("category_l1",))

    instruments_by_l3: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for instrument in instruments:
        instruments_by_l3[(instrument["category_l1"], instrument["category_l2"], instrument["category_l3"])].append(instrument)
    for children in instruments_by_l3.values():
        _sort_items(children, "name")

    l3_by_l2: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for l3 in l3_items:
        l3["children"] = instruments_by_l3[(l3["category_l1"], l3["category_l2"], l3["category_l3"])]
        l3["child_count"] = len(l3["children"])
        _assign_envelope(l3, l3["children"])
        l3_by_l2[(l3["category_l1"], l3["category_l2"])].append(l3)
    for children in l3_by_l2.values():
        _sort_items(children, "category_l3")

    l2_by_l1: dict[str, list[dict]] = defaultdict(list)
    for l2 in l2_items:
        l2["children"] = l3_by_l2[(l2["category_l1"], l2["category_l2"])]
        l2["child_count"] = len(l2["children"])
        _assign_envelope(l2, l2["children"])
        l2_by_l1[l2["category_l1"]].append(l2)
    for children in l2_by_l1.values():
        _sort_items(children, "category_l2")

    groups = [
        {
            "category_l1": l1,
            "count": len(items),
            "items": items,
            "priority_l1": min(item["priority_l1"] for item in items),
        }
        for l1, items in l2_by_l1.items()
    ]
    groups.sort(key=lambda group: (group["priority_l1"], group["category_l1"]))
    return {
        "as_of": max((item["as_of"] for item in l2_items), default=None),
        "groups": groups,
        "secondary_count": len(l2_items),
        "category_count": len(l3_items),
        "instrument_count": int(source["symbol"].nunique()),
    }


@router.get("", response_class=HTMLResponse)
async def subject_market_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        name="subject_market.html", request=request, context={"title": "标的看板"}
    )


@router.get("/api/dashboard")
async def subject_market_dashboard() -> dict:
    global _dashboard_cache
    db = get_db()
    revision = db.get_market_dashboard_revision()
    if _dashboard_cache is not None and _dashboard_cache[0] == revision:
        return _dashboard_cache[1]
    payload = build_subject_dashboard_payload(db)
    _dashboard_cache = (revision, payload)
    return payload


# ---------------------------------------------------------------------------
# Intraday (real-time) endpoints
# ---------------------------------------------------------------------------

_intraday_jobs: dict[str, dict[str, Any]] = {}
_intraday_jobs_lock = threading.Lock()
_INTRADAY_JOB_TTL_SECONDS = 300  # 5 minutes


def _cleanup_expired_jobs() -> None:
    """Remove intraday jobs older than TTL."""
    now = _time.monotonic()
    with _intraday_jobs_lock:
        expired = [
            jid for jid, job in _intraday_jobs.items()
            if now - job.get("_created", now) > _INTRADAY_JOB_TTL_SECONDS
        ]
        for jid in expired:
            del _intraday_jobs[jid]


def _create_intraday_job() -> str:
    _cleanup_expired_jobs()
    job_id = uuid.uuid4().hex[:12]
    with _intraday_jobs_lock:
        _intraday_jobs[job_id] = {
            "status": "started",
            "percent": 0.0,
            "message": "正在准备…",
            "result": None,
            "_created": _time.monotonic(),
        }
    return job_id


@router.get("/api/trading-status")
async def get_trading_status() -> dict:
    """Return current A-share trading session status."""
    return trading_session_status()


@router.post("/api/intraday-dashboard/start")
async def start_intraday_dashboard() -> dict:
    """Launch an intraday dashboard computation in the background."""
    now = datetime.now()
    if not is_trading_day(now.date()):
        raise HTTPException(status_code=400, detail="今日非交易日")
    # 午间休盘（11:30-13:00）也允许启动 —— 实时报价在休盘期间仍然有效。
    if not is_realtime_available(now):
        raise HTTPException(status_code=400, detail="当前非交易时段（9:30-15:00，含午间休盘）")

    job_id = _create_intraday_job()

    def _run() -> None:
        try:
            db = get_db()
            symbols = db.list_market_symbols(price_mode="qfq")
            if not symbols:
                with _intraday_jobs_lock:
                    _intraday_jobs[job_id]["status"] = "error"
                    _intraday_jobs[job_id]["message"] = "本地无日K数据"
                return

            # Filter to classified instruments only.
            metadata_map = db.get_instrument_metadata_map()
            classified = [
                s for s in symbols
                if s in metadata_map
                and str(metadata_map[s].get("category_l1", "")).strip()
                and str(metadata_map[s].get("category_l2", "")).strip()
                and str(metadata_map[s].get("category_l3", "")).strip()
            ]
            if not classified:
                with _intraday_jobs_lock:
                    _intraday_jobs[job_id]["status"] = "error"
                    _intraday_jobs[job_id]["message"] = "无完整分类的标的"
                return

            data_service = DataService()
            trend_config = _trend_config()

            def on_progress(update: dict) -> None:
                with _intraday_jobs_lock:
                    job = _intraday_jobs.get(job_id)
                    if job:
                        job["percent"] = float(update.get("percent", 0))
                        job["message"] = str(update.get("message", ""))

            payload = build_intraday_dashboard(
                classified,
                db=db,
                data_service=data_service,
                trend_config=trend_config,
                progress_callback=on_progress,
            )
            with _intraday_jobs_lock:
                job = _intraday_jobs.get(job_id)
                if job:
                    job["status"] = "done"
                    job["percent"] = 1.0
                    job["message"] = "完成"
                    job["result"] = payload
        except Exception as exc:
            with _intraday_jobs_lock:
                job = _intraday_jobs.get(job_id)
                if job:
                    job["status"] = "error"
                    job["message"] = str(exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return {"job_id": job_id, "status": "started"}


@router.get("/api/intraday-dashboard/progress/{job_id}")
async def get_intraday_dashboard_progress(job_id: str) -> dict:
    """Poll the progress of an intraday dashboard job."""
    with _intraday_jobs_lock:
        job = _intraday_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在或已过期")
    return {
        "job_id": job_id,
        "status": job["status"],
        "percent": job["percent"],
        "message": job["message"],
    }


@router.get("/api/intraday-dashboard/result/{job_id}")
async def get_intraday_dashboard_result(job_id: str) -> dict:
    """Retrieve the result of a completed intraday dashboard job."""
    with _intraday_jobs_lock:
        job = _intraday_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="任务不存在或已过期")
    if job["status"] == "error":
        raise HTTPException(status_code=500, detail=job.get("message", "盘中计算失败"))
    if job["status"] != "done":
        raise HTTPException(status_code=202, detail="任务尚未完成")
    return job["result"]
