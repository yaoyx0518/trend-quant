from __future__ import annotations

import logging
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Callable

import pandas as pd
import yaml
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from core.benchmarks import benchmark_instruments
from data.service import DataService
from data.storage.db import get_db
from data.storage.market_store import MarketStore
from data.storage.runtime_store import RuntimeStore

router = APIRouter(prefix="/instruments", tags=["instruments"])
templates = Jinja2Templates(directory="web/templates")
market_store = MarketStore()
runtime_store = RuntimeStore()
logger = logging.getLogger(__name__)


class InstrumentBackfillRequest(BaseModel):
    start_date: str = Field(default="")
    end_date: str = Field(default="")
    adjust: str = Field(default="")


class InstrumentBulkBackfillItem(BaseModel):
    symbol: str
    start_date: str = Field(default="")


class InstrumentBulkBackfillRequest(BaseModel):
    items: list[InstrumentBulkBackfillItem] = Field(default_factory=list)
    end_date: str = Field(default="")
    adjust: str = Field(default="")


class InstrumentNameLookupRequest(BaseModel):
    symbol: str


class InstrumentAddRequest(BaseModel):
    symbol: str
    name: str = Field(default="")
    category_l1: str
    category_l2: str
    category_l3: str
    end_date: str = Field(default="")
    adjust: str = Field(default="")


class InstrumentUpdateRequest(BaseModel):
    category_l1: str
    category_l2: str
    category_l3: str


_config_write_lock = threading.Lock()


def _empty_bulk_status() -> dict:
    return {
        "job_id": None,
        "status": "idle",
        "started_at": None,
        "finished_at": None,
        "progress_current": 0,
        "progress_total": 0,
        "current_symbol": None,
        "message": "空闲。",
        "error": None,
        "summary": {
            "total": 0,
            "finished": 0,
            "updated": 0,
            "no_data": 0,
            "failed": 0,
            "added_rows": 0,
        },
    }


class BulkBackfillJobManager:
    def __init__(
        self,
        data_service_factory: Callable[[list[str] | None], DataService] | None = None,
        runtime_store_obj: RuntimeStore | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._status = _empty_bulk_status()
        self._data_service_factory = data_service_factory or (
            lambda provider_priority: DataService(provider_priority=provider_priority)
        )
        self._runtime_store = runtime_store_obj or runtime_store

    def snapshot(self) -> dict:
        with self._lock:
            return self._copy_status()

    def is_running(self) -> bool:
        with self._lock:
            return self._status.get("status") == "running"

    def start(
        self,
        *,
        items: list[dict],
        end_date: date,
        adjust: str,
        provider_priority: list[str] | None,
    ) -> tuple[bool, dict]:
        if not items:
            raise HTTPException(status_code=400, detail="没有可补齐的标的")

        now = datetime.now()
        job_id = now.strftime("%Y%m%d%H%M%S%f")
        with self._lock:
            if self._status.get("status") == "running":
                return False, self._copy_status()

            self._status = {
                "job_id": job_id,
                "status": "running",
                "started_at": now.isoformat(),
                "finished_at": None,
                "progress_current": 0,
                "progress_total": len(items),
                "current_symbol": None,
                "message": f"后台补齐任务已启动，共 {len(items)} 个标的。",
                "error": None,
                "summary": {
                    "total": len(items),
                    "finished": 0,
                    "updated": 0,
                    "up_to_date": 0,
                    "no_data": 0,
                    "failed": 0,
                    "added_rows": 0,
                    "attempt": 1,
                    "max_attempts": 4,
                    "retrying": 0,
                },
                "results": [],
                "adjust": adjust,
                "requested_end": end_date.isoformat(),
            }
            snapshot = self._copy_status()

        thread = threading.Thread(
            target=self._run,
            kwargs={
                "job_id": job_id,
                "items": items,
                "end_date": end_date,
                "adjust": adjust,
                "provider_priority": provider_priority,
            },
            daemon=True,
        )
        thread.start()
        return True, snapshot

    def _copy_status(self) -> dict:
        status = dict(self._status)
        status["summary"] = dict(self._status.get("summary") or {})
        status["results"] = list(self._status.get("results") or [])
        return status

    def _run(
        self,
        *,
        job_id: str,
        items: list[dict],
        end_date: date,
        adjust: str,
        provider_priority: list[str] | None,
    ) -> None:
        data_service = self._data_service_factory(provider_priority)
        try:
            def _on_progress(update: dict) -> None:
                with self._lock:
                    if self._status.get("job_id") != job_id:
                        return
                    event = str(update.get("event") or "")
                    summary = self._status["summary"]
                    summary["attempt"] = int(update.get("attempt") or summary.get("attempt") or 1)
                    summary["max_attempts"] = int(update.get("max_attempts") or summary.get("max_attempts") or 4)
                    if event == "retry_sleep":
                        summary["retrying"] = int(update.get("remaining") or 0)
                    else:
                        summary["retrying"] = int(summary.get("retrying") or 0)
                    self._status["progress_current"] = int(
                        update.get("finished") or self._status.get("progress_current") or 0
                    )
                    self._status["progress_total"] = int(update.get("total") or len(items))
                    if event == "attempt_start":
                        self._status["message"] = (
                            f"第 {summary['attempt']}/{summary['max_attempts']} 轮批量补齐，"
                            f"待处理 {int(update.get('remaining') or 0)} 个标的。"
                        )
                    elif event == "request_start":
                        symbols = list(update.get("symbols") or [])
                        self._status["current_symbol"] = symbols[0] if symbols else None
                        self._status["message"] = (
                            f"正在批量请求第 {summary['attempt']}/{summary['max_attempts']} 轮 "
                            f"第 {int(update.get('chunk_index') or 0)}/{int(update.get('chunk_total') or 0)} 批，"
                            f"本批 {len(symbols)} 个标的。"
                        )
                    elif event == "item_done":
                        symbol = str(update.get("symbol") or "")
                        self._status["current_symbol"] = symbol or None
                        self._status["message"] = (
                            f"已完成 {self._status['progress_current']}/{self._status['progress_total']}：{symbol}"
                        )
                    elif event == "retry_sleep":
                        self._status["current_symbol"] = None
                        self._status["message"] = (
                            f"本轮有 {int(update.get('remaining') or 0)} 个标的失败，"
                            f"{float(update.get('wait_seconds') or 0):.1f} 秒后只重试失败标的。"
                        )

            result_payloads = data_service.backfill_daily_histories(
                items=items,
                end_date=end_date,
                adjust=adjust,
                max_retries=3,
                batch_size=100,
                request_interval_seconds=2.0,
                retry_delay_seconds=2.0,
                progress_callback=_on_progress,
            )

            for result_payload in result_payloads:
                with self._lock:
                    if self._status.get("job_id") != job_id:
                        return
                    summary = self._status["summary"]
                    summary["finished"] = int(summary.get("finished", 0)) + 1
                    if result_payload["ok"]:
                        result = result_payload["result"]
                        status = str(result.get("status") or "")
                        if status == "no_data":
                            summary["no_data"] = int(summary.get("no_data", 0)) + 1
                        elif status == "up_to_date":
                            summary["up_to_date"] = int(summary.get("up_to_date", 0)) + 1
                        else:
                            summary["updated"] = int(summary.get("updated", 0)) + 1
                        summary["added_rows"] = int(summary.get("added_rows", 0)) + int(
                            result.get("added_rows") or 0
                        )
                    else:
                        summary["failed"] = int(summary.get("failed", 0)) + 1

                    self._status.setdefault("results", []).append(result_payload)

            with self._lock:
                if self._status.get("job_id") != job_id:
                    return
                summary = self._status["summary"]
                all_failed = (
                    int(summary.get("total") or len(items)) > 0
                    and int(summary.get("failed") or 0) == int(summary.get("total") or len(items))
                    and int(summary.get("updated") or 0) == 0
                    and int(summary.get("up_to_date") or 0) == 0
                    and int(summary.get("no_data") or 0) == 0
                )
                first_error = ""
                if all_failed:
                    for result_payload in self._status.get("results") or []:
                        if not result_payload.get("ok"):
                            first_error = str(result_payload.get("error") or "")
                            break
                self._status["status"] = "failed" if all_failed else "completed"
                self._status["progress_current"] = int(summary.get("total") or len(items))
                self._status["progress_total"] = int(summary.get("total") or len(items))
                self._status["current_symbol"] = None
                self._status["finished_at"] = datetime.now().isoformat()
                if all_failed:
                    self._status["error"] = first_error or "所有标的补齐失败"
                    self._status["message"] = (
                        f"后台补齐失败：共 {summary.get('total', 0)} 个标的全部失败。"
                        f"{first_error}"
                    )
                else:
                    self._status["message"] = (
                        f"后台补齐完成：共 {summary.get('total', 0)} 个标的，"
                        f"已最新 {summary.get('up_to_date', 0)} 个，"
                        f"失败 {summary.get('failed', 0)} 个，"
                        f"未获取到数据 {summary.get('no_data', 0)} 个，"
                        f"新增 {int(summary.get('added_rows', 0)):,} 行。"
                    )
                final_status = self._copy_status()
            self._runtime_store.write_json(f"advice/instrument_bulk_backfill_{job_id}.json", final_status)
        except Exception as exc:
            logger.exception("Instrument bulk backfill job_id=%s failed", job_id)
            with self._lock:
                if self._status.get("job_id") == job_id:
                    self._status["status"] = "failed"
                    self._status["finished_at"] = datetime.now().isoformat()
                    self._status["error"] = str(exc)
                    self._status["message"] = f"后台补齐任务失败：{exc}"
        finally:
            close = getattr(data_service, "close", None)
            if callable(close):
                close()


bulk_backfill_manager = BulkBackfillJobManager()


def _empty_add_status() -> dict:
    return {
        "job_id": None,
        "status": "idle",
        "started_at": None,
        "finished_at": None,
        "progress_current": 0,
        "progress_total": 0,
        "current_symbol": None,
        "message": "空闲。",
        "error": None,
        "summary": {
            "symbol": None,
            "name": None,
            "metadata_saved": 0,
            "config_saved": 0,
            "added_rows": 0,
            "backfill_status": None,
        },
    }


class InstrumentAddJobManager:
    def __init__(
        self,
        data_service_factory: Callable[[list[str] | None], DataService] | None = None,
        runtime_store_obj: RuntimeStore | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._status = _empty_add_status()
        self._pending_symbols: set[str] = set()
        self._data_service_factory = data_service_factory or (
            lambda provider_priority: DataService(provider_priority=provider_priority)
        )
        self._runtime_store = runtime_store_obj or runtime_store

    def snapshot(self) -> dict:
        with self._lock:
            return self._copy_status()

    def is_running(self) -> bool:
        with self._lock:
            return self._status.get("status") == "running"

    def is_symbol_pending(self, symbol: str) -> bool:
        normalized = _normalize_symbol(symbol)
        with self._lock:
            return normalized in self._pending_symbols

    def start(
        self,
        *,
        item: dict,
        end_date: date,
        adjust: str,
        provider_priority: list[str] | None,
    ) -> tuple[bool, dict]:
        symbol = _normalize_symbol(item.get("symbol", ""))
        if not symbol:
            raise HTTPException(status_code=400, detail="标的无效")

        now = datetime.now()
        job_id = now.strftime("%Y%m%d%H%M%S%f")
        with self._lock:
            if self._status.get("status") == "running":
                return False, self._copy_status()
            if symbol in self._pending_symbols:
                return False, self._copy_status()

            self._pending_symbols.add(symbol)
            self._status = {
                "job_id": job_id,
                "status": "running",
                "started_at": now.isoformat(),
                "finished_at": None,
                "progress_current": 0,
                "progress_total": 3,
                "current_symbol": symbol,
                "message": f"新增标的任务已启动：{symbol}。",
                "error": None,
                "summary": {
                    "symbol": symbol,
                    "name": str(item.get("name") or "").strip(),
                    "metadata_saved": 0,
                    "config_saved": 0,
                    "added_rows": 0,
                    "backfill_status": None,
                },
                "result": None,
                "adjust": adjust,
                "requested_end": end_date.isoformat(),
            }
            snapshot = self._copy_status()

        thread = threading.Thread(
            target=self._run,
            kwargs={
                "job_id": job_id,
                "item": {**item, "symbol": symbol},
                "end_date": end_date,
                "adjust": adjust,
                "provider_priority": provider_priority,
            },
            daemon=True,
        )
        thread.start()
        return True, snapshot

    def _copy_status(self) -> dict:
        status = dict(self._status)
        status["summary"] = dict(self._status.get("summary") or {})
        status["result"] = dict(self._status.get("result") or {}) if self._status.get("result") else None
        return status

    def _set_progress(self, job_id: str, current: int, message: str) -> None:
        with self._lock:
            if self._status.get("job_id") != job_id:
                return
            self._status["progress_current"] = current
            self._status["message"] = message

    def _run(
        self,
        *,
        job_id: str,
        item: dict,
        end_date: date,
        adjust: str,
        provider_priority: list[str] | None,
    ) -> None:
        symbol = str(item.get("symbol") or "").strip().upper()
        data_service = self._data_service_factory(provider_priority)
        try:
            self._set_progress(job_id, 0, f"正在写入 {symbol} 的配置与分类信息。")
            record = _build_new_instrument_record(item)
            saved = _append_instrument_config(record)
            with self._lock:
                if self._status.get("job_id") == job_id:
                    summary = self._status["summary"]
                    summary["name"] = record.get("name")
                    summary["metadata_saved"] = saved
                    summary["config_saved"] = saved
                    self._status["progress_current"] = 1
                    self._status["message"] = f"{symbol} 已写入配置，正在补齐历史行情。"

            result = data_service.backfill_daily_history(
                symbol=symbol,
                start_date=date(2020, 1, 1),
                end_date=end_date,
                adjust=adjust,
            )
            result["adjust"] = adjust
            result["symbol"] = symbol
            result["request_adjusted_to_earliest_available"] = bool(
                result.get("fetched_start")
                and result.get("requested_start")
                and str(result.get("fetched_start")) > str(result.get("requested_start"))
            )

            with self._lock:
                if self._status.get("job_id") != job_id:
                    return
                summary = self._status["summary"]
                summary["added_rows"] = int(result.get("added_rows") or 0)
                summary["backfill_status"] = str(result.get("status") or "")
                self._status["status"] = "completed"
                self._status["finished_at"] = datetime.now().isoformat()
                self._status["progress_current"] = 3
                self._status["current_symbol"] = None
                self._status["message"] = (
                    f"新增标的完成：{symbol}，新增 "
                    f"{int(result.get('added_rows') or 0):,} 行历史行情。"
                )
                self._status["result"] = result
                final_status = self._copy_status()
            self._runtime_store.write_json(f"advice/instrument_add_{job_id}.json", final_status)
        except Exception as exc:
            logger.exception("Instrument add job_id=%s failed for %s", job_id, symbol)
            with self._lock:
                if self._status.get("job_id") == job_id:
                    self._status["status"] = "failed"
                    self._status["finished_at"] = datetime.now().isoformat()
                    self._status["error"] = str(exc)
                    self._status["message"] = f"新增标的任务失败：{exc}"
        finally:
            close = getattr(data_service, "close", None)
            if callable(close):
                close()
            with self._lock:
                self._pending_symbols.discard(symbol)


add_instrument_manager = InstrumentAddJobManager()


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
        code, suffix = text.split(".", 1)
        if suffix == "SH":
            suffix = "SS"
        return f"{code}.{suffix}"
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
    out: dict[str, str] = {}
    for item in get_db().list_instrument_metadata():
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol == "":
            continue
        out[symbol] = str(item.get("name", "") or "").strip()
    for item in benchmark_instruments():
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol:
            out.setdefault(symbol, str(item.get("name", "") or "").strip())
    return out


def _config_items() -> list[dict]:
    return [dict(item) for item in get_db().list_instrument_metadata()]


def _known_managed_symbols() -> set[str]:
    symbols: set[str] = set()
    for item in _config_items():
        if isinstance(item, dict):
            symbol = _normalize_symbol(item.get("symbol", ""))
            if symbol:
                symbols.add(symbol)
    for item in benchmark_instruments():
        symbol = _normalize_symbol(item.get("symbol", ""))
        if symbol:
            symbols.add(symbol)

    db = get_db()
    symbols.update(str(item.get("symbol") or "").strip().upper() for item in db.list_instrument_metadata())
    symbols.update(str(symbol or "").strip().upper() for symbol in db.list_market_symbols())
    return {symbol for symbol in symbols if symbol}


def _category_path_from_parts(l1: str, l2: str = "", l3: str = "") -> str:
    return "-".join(part for part in [l1, l2, l3] if str(part or "").strip())


def _category_priority_map() -> dict[str, int | None]:
    try:
        rows = get_db().list_instrument_categories()
    except RuntimeError:
        rows = []
    return {
        str(row.get("path") or "").strip(): row.get("priority")
        for row in rows
        if str(row.get("path") or "").strip()
    }


def _next_sort_order(config_items: list[dict] | None = None) -> int:
    values: list[int] = []
    for item in config_items if config_items is not None else _config_items():
        if isinstance(item, dict):
            try:
                values.append(int(item.get("sort_order") or 0))
            except (TypeError, ValueError):
                pass
    try:
        for item in get_db().list_instrument_metadata():
            try:
                values.append(int(item.get("sort_order") or 0))
            except (TypeError, ValueError):
                pass
    except RuntimeError:
        pass
    return max(values or [0]) + 1


def _build_new_instrument_record(item: dict) -> dict:
    symbol = _normalize_symbol(item.get("symbol", ""))
    name = str(item.get("name") or "").strip()
    l1 = str(item.get("category_l1") or "").strip()
    l2 = str(item.get("category_l2") or "").strip()
    l3 = str(item.get("category_l3") or "").strip()
    if not symbol:
        raise ValueError("标的无效")
    if not name:
        raise ValueError("标的名称为空，请先查询名称")
    if not (l1 and l2 and l3):
        raise ValueError("一二三级类目均必选")

    priorities = _category_priority_map()
    asset_type = "stock" if l1 == "股票" else "etf"
    return {
        "symbol": symbol,
        "name": name,
        "enabled": True,
        "risk_budget_pct": 0.01,
        "stop_atr_mul": 1.5,
        "asset_type": asset_type,
        "category_l1": l1,
        "category_l2": l2,
        "category_l3": l3,
        "factor_tags": [],
        "region_tag": "",
        "priority_l1": priorities.get(_category_path_from_parts(l1)),
        "priority_l2": priorities.get(_category_path_from_parts(l1, l2)),
        "priority_l3": priorities.get(_category_path_from_parts(l1, l2, l3)),
        "sort_order": _next_sort_order(),
        "source": "manual_add",
    }


def _append_instrument_config(record: dict) -> int:
    """Insert a new managed instrument into the metadata table (dup-rejecting)."""
    db = get_db()
    symbol = _normalize_symbol(record.get("symbol", ""))
    if db.get_instrument_metadata(symbol) is not None:
        raise ValueError(f"{symbol} 已在标的配置中")

    config_record = {
        "symbol": symbol,
        "name": str(record.get("name") or "").strip(),
        "enabled": bool(record.get("enabled", True)),
        "risk_budget_pct": record.get("risk_budget_pct", 0.01),
        "stop_atr_mul": record.get("stop_atr_mul", 1.5),
        "asset_type": str(record.get("asset_type") or "etf"),
        "category_l1": str(record.get("category_l1") or "").strip(),
        "category_l2": str(record.get("category_l2") or "").strip(),
        "category_l3": str(record.get("category_l3") or "").strip(),
        "factor_tags": record.get("factor_tags") or [],
        "region_tag": str(record.get("region_tag") or ""),
        "priority_l1": record.get("priority_l1"),
        "priority_l2": record.get("priority_l2"),
        "priority_l3": record.get("priority_l3"),
        "sort_order": record.get("sort_order"),
        "source": str(record.get("source") or ""),
    }
    return db.save_instrument_metadata([config_record])


def _category_options() -> list[dict]:
    db = get_db()
    rows = db.list_instrument_categories()
    if rows:
        return rows

    categories: dict[str, dict] = {}
    for item in [*db.list_instrument_metadata(), *_config_items()]:
        if not isinstance(item, dict):
            continue
        l1 = str(item.get("category_l1") or "").strip()
        l2 = str(item.get("category_l2") or "").strip()
        l3 = str(item.get("category_l3") or "").strip()
        if l1:
            categories.setdefault(
                l1,
                {"path": l1, "level": 1, "name": l1, "parent_path": "", "priority": item.get("priority_l1")},
            )
        if l1 and l2:
            path = _category_path_from_parts(l1, l2)
            categories.setdefault(
                path,
                {"path": path, "level": 2, "name": l2, "parent_path": l1, "priority": item.get("priority_l2")},
            )
        if l1 and l2 and l3:
            parent = _category_path_from_parts(l1, l2)
            path = _category_path_from_parts(l1, l2, l3)
            categories.setdefault(
                path,
                {"path": path, "level": 3, "name": l3, "parent_path": parent, "priority": item.get("priority_l3")},
            )
    return sorted(
        categories.values(),
        key=lambda item: (
            int(item.get("level") or 0),
            str(item.get("parent_path") or ""),
            int(item.get("priority") or 9999),
            str(item.get("name") or ""),
        ),
    )


def _category_path(meta: dict | None) -> str:
    if not meta:
        return ""
    parts = [
        str(meta.get("category_l1") or "").strip(),
        str(meta.get("category_l2") or "").strip(),
        str(meta.get("category_l3") or "").strip(),
    ]
    return "-".join(part for part in parts if part)


def _metadata_priority(meta: dict | None) -> tuple:
    if not meta:
        return (1, 9999, 9999, 9999, 999999)
    return (
        0,
        int(meta.get("priority_l1") or 9999),
        int(meta.get("priority_l2") or 9999),
        int(meta.get("priority_l3") or 9999),
        int(meta.get("sort_order") or 999999),
    )


def _provider_priority_from_request(request: Request) -> list[str] | None:
    if hasattr(request.app.state, "settings"):
        return list(getattr(request.app.state.settings.app, "data_provider_priority", []) or [])
    return None


def _default_adjust() -> str:
    strategy_payload = _load_yaml("config/strategy.yaml")
    strategy_cfg = strategy_payload.get("strategy", {}) if isinstance(strategy_payload, dict) else {}
    return str(strategy_cfg.get("adjust", "qfq")) if isinstance(strategy_cfg, dict) else "qfq"


@router.get("", response_class=HTMLResponse)
async def instruments_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        name="instruments.html",
        request=request,
        context={"title": "标的管理"},
    )


@router.get("/api/categories")
async def list_categories() -> dict:
    items = _category_options()
    return {"ok": True, "items": items, "count": len(items)}


@router.post("/api/lookup")
async def lookup_instrument_name(payload: InstrumentNameLookupRequest, request: Request) -> dict:
    normalized_symbol = _normalize_symbol(payload.symbol)
    if normalized_symbol == "":
        raise HTTPException(status_code=400, detail="标的代码必填")
    if add_instrument_manager.is_symbol_pending(normalized_symbol):
        raise HTTPException(status_code=409, detail=f"{normalized_symbol} 正在新增补充中")
    if normalized_symbol in _known_managed_symbols():
        raise HTTPException(status_code=409, detail=f"{normalized_symbol} 已被管理")

    data_service = DataService(provider_priority=_provider_priority_from_request(request))
    try:
        result = data_service.fetch_instrument_name(normalized_symbol)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"名称查询失败：{exc}") from exc
    finally:
        data_service.close()

    return {
        "ok": True,
        "symbol": normalized_symbol,
        "code": _symbol_to_code(normalized_symbol),
        "exchange": _symbol_suffix(normalized_symbol),
        "name": result.get("name", ""),
        "provider": result.get("provider", ""),
        "ts": result.get("ts"),
    }


@router.post("/api/add")
async def start_add_instrument(payload: InstrumentAddRequest, request: Request) -> dict:
    normalized_symbol = _normalize_symbol(payload.symbol)
    if normalized_symbol == "":
        raise HTTPException(status_code=400, detail="标的代码必填")
    if add_instrument_manager.is_symbol_pending(normalized_symbol):
        raise HTTPException(status_code=409, detail=f"{normalized_symbol} 正在新增补充中")
    if add_instrument_manager.is_running():
        raise HTTPException(status_code=409, detail="已有新增标的任务正在运行")
    if normalized_symbol in _known_managed_symbols():
        raise HTTPException(status_code=409, detail=f"{normalized_symbol} 已被管理")

    category_values = [
        str(payload.category_l1 or "").strip(),
        str(payload.category_l2 or "").strip(),
        str(payload.category_l3 or "").strip(),
    ]
    if not all(category_values):
        raise HTTPException(status_code=400, detail="一二三级类目均必选")

    valid_paths = {str(item.get("path") or "").strip() for item in _category_options()}
    category_path = _category_path_from_parts(*category_values)
    if category_path not in valid_paths:
        raise HTTPException(status_code=400, detail="类目组合不存在，请重新选择")

    try:
        end_date = _to_date(payload.end_date, date.today())
    except ValueError:
        raise HTTPException(status_code=400, detail="日期格式必须是 YYYY-MM-DD")

    adjust = str(payload.adjust or _default_adjust()).strip().lower() or "qfq"
    item = {
        "symbol": normalized_symbol,
        "name": str(payload.name or "").strip(),
        "category_l1": category_values[0],
        "category_l2": category_values[1],
        "category_l3": category_values[2],
    }
    started, status = add_instrument_manager.start(
        item=item,
        end_date=end_date,
        adjust=adjust,
        provider_priority=_provider_priority_from_request(request),
    )
    return {"ok": True, "started": started, "job": status}


@router.get("/api/add/status")
async def get_add_instrument_status() -> dict:
    return {"ok": True, "job": add_instrument_manager.snapshot()}


@router.get("/api/list")
async def list_instruments() -> dict:
    config_by_symbol: dict[str, dict] = {}
    for item in _config_items():
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol == "":
            continue
        config_by_symbol[symbol] = item

    benchmark_by_symbol = {
        str(item.get("symbol", "")).strip().upper(): item
        for item in benchmark_instruments()
        if str(item.get("symbol", "")).strip()
    }

    db = get_db()
    name_map = _config_name_map()
    metadata_by_symbol = db.get_instrument_metadata_map()

    known_symbols = set(config_by_symbol.keys()) | set(benchmark_by_symbol.keys()) | set(metadata_by_symbol.keys())
    for symbol in db.list_market_symbols():
        known_symbols.add(symbol.upper())

    items: list[dict] = []
    for symbol in sorted(known_symbols):
        meta = metadata_by_symbol.get(symbol)
        summary = db.get_market_data_summary(symbol)
        rows = summary.get("rows", 0)
        local_start = summary.get("start")
        local_end = summary.get("end")

        cfg = config_by_symbol.get(symbol, {})
        in_config = bool(cfg)
        is_benchmark = symbol in benchmark_by_symbol
        enabled = bool(cfg.get("enabled", False)) if in_config else False
        name = str((meta or {}).get("name") or name_map.get(symbol, "") or "")
        sort_key = _metadata_priority(meta)
        items.append(
            {
                "symbol": symbol,
                "code": _symbol_to_code(symbol),
                "exchange": _symbol_suffix(symbol),
                "name": name,
                "category_l1": str((meta or {}).get("category_l1") or ""),
                "category_l2": str((meta or {}).get("category_l2") or ""),
                "category_l3": str((meta or {}).get("category_l3") or ""),
                "category_path": _category_path(meta),
                "factor_tags": list((meta or {}).get("factor_tags") or []),
                "region_tag": str((meta or {}).get("region_tag") or ""),
                "priority_l1": sort_key[1],
                "priority_l2": sort_key[2],
                "priority_l3": sort_key[3],
                "sort_order": sort_key[4],
                "enabled": enabled,
                "in_config": in_config,
                "is_benchmark": is_benchmark,
                "rows": rows,
                "local_start_date": local_start,
                "local_end_date": local_end,
                "path": f"sqlite/{symbol}",
            }
        )

    items.sort(
        key=lambda x: (
            1 if not x.get("category_path") else 0,
            int(x.get("priority_l1") or 9999),
            int(x.get("priority_l2") or 9999),
            int(x.get("priority_l3") or 9999),
            int(x.get("sort_order") or 999999),
            str(x.get("symbol", "")),
        )
    )
    return {"items": items, "count": len(items), "as_of": datetime.now().isoformat()}


@router.post("/api/{symbol}/update")
async def update_instrument(symbol: str, payload: InstrumentUpdateRequest) -> dict:
    normalized_symbol = _normalize_symbol(symbol)
    if normalized_symbol == "":
        raise HTTPException(status_code=400, detail="标的无效")

    db = get_db()
    existing_meta = db.get_instrument_metadata(normalized_symbol)
    if not existing_meta:
        raise HTTPException(status_code=404, detail=f"{normalized_symbol} 不在标的管理列表中")

    category_values = [
        str(payload.category_l1 or "").strip(),
        str(payload.category_l2 or "").strip(),
        str(payload.category_l3 or "").strip(),
    ]
    if not all(category_values):
        raise HTTPException(status_code=400, detail="一二三级类目均必选")

    valid_paths = {str(item.get("path") or "").strip() for item in _category_options()}
    category_path = _category_path_from_parts(*category_values)
    if category_path not in valid_paths:
        raise HTTPException(status_code=400, detail="类目组合不存在，请重新选择")

    priorities = _category_priority_map()
    updates = {
        "category_l1": category_values[0],
        "category_l2": category_values[1],
        "category_l3": category_values[2],
        "priority_l1": priorities.get(_category_path_from_parts(category_values[0])),
        "priority_l2": priorities.get(_category_path_from_parts(category_values[0], category_values[1])),
        "priority_l3": priorities.get(category_path),
        "asset_type": "stock" if category_values[0] == "股票" else "etf",
    }

    meta = dict(existing_meta or {})
    meta.update(updates)
    meta["symbol"] = normalized_symbol
    if not str(meta.get("name") or "").strip():
        meta["name"] = _config_name_map().get(normalized_symbol, "")
    if meta.get("sort_order") is None:
        meta["sort_order"] = _next_sort_order()
    if not str(meta.get("source") or "").strip():
        meta["source"] = "manual_edit"
    saved = db.save_instrument_metadata([meta])

    return {
        "ok": True,
        "symbol": normalized_symbol,
        "category_l1": updates["category_l1"],
        "category_l2": updates["category_l2"],
        "category_l3": updates["category_l3"],
        "category_path": category_path,
        "priority_l1": updates["priority_l1"],
        "priority_l2": updates["priority_l2"],
        "priority_l3": updates["priority_l3"],
        "config_saved": saved,
        "metadata_saved": saved,
    }


@router.post("/api/{symbol}/backfill")
async def backfill_instrument(symbol: str, payload: InstrumentBackfillRequest, request: Request) -> dict:
    normalized_symbol = _normalize_symbol(symbol)
    if normalized_symbol == "":
        raise HTTPException(status_code=400, detail="标的无效")

    if str(payload.start_date or "").strip() == "":
        raise HTTPException(status_code=400, detail="开始日期必填")

    try:
        start_date = _to_date(payload.start_date, date(2020, 1, 1))
        end_date = _to_date(payload.end_date, date.today())
    except ValueError:
        raise HTTPException(status_code=400, detail="日期格式必须是 YYYY-MM-DD")

    adjust = str(payload.adjust or _default_adjust()).strip().lower() or "qfq"

    data_service = DataService(provider_priority=_provider_priority_from_request(request))
    try:
        result = data_service.backfill_daily_history(
            symbol=normalized_symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )
    finally:
        data_service.close()
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


@router.post("/api/backfill-all")
async def start_bulk_backfill(payload: InstrumentBulkBackfillRequest, request: Request) -> dict:
    try:
        end_date = _to_date(payload.end_date, date.today())
    except ValueError:
        raise HTTPException(status_code=400, detail="日期格式必须是 YYYY-MM-DD")

    adjust = str(payload.adjust or _default_adjust()).strip().lower() or "qfq"
    items: list[dict] = []
    seen_symbols: set[str] = set()
    for raw_item in payload.items:
        symbol = _normalize_symbol(raw_item.symbol)
        if symbol == "" or symbol in seen_symbols:
            continue
        seen_symbols.add(symbol)
        try:
            start_date = _to_date(raw_item.start_date, date(2020, 1, 1))
        except ValueError:
            raise HTTPException(status_code=400, detail=f"{symbol} 的开始日期格式必须是 YYYY-MM-DD")
        items.append({"symbol": symbol, "start_date": start_date})

    started, status = bulk_backfill_manager.start(
        items=items,
        end_date=end_date,
        adjust=adjust,
        provider_priority=_provider_priority_from_request(request),
    )
    return {"ok": True, "started": started, "job": status}


@router.get("/api/backfill-all/status")
async def get_bulk_backfill_status() -> dict:
    return {"ok": True, "job": bulk_backfill_manager.snapshot()}


@router.get("/api/daily-update/status")
async def daily_update_status() -> dict:
    """Latest 16:30 daily data update status for the global notification bar."""
    status = runtime_store.read_json("daily_update_status.json")
    if not status:
        return {"ts": None, "completed": False, "message": "暂无更新记录"}
    return status
