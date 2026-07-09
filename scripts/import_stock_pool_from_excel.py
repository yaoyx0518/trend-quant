from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from data.provider_tickflow import TickFlowProvider
from data.storage.db import init_db
from data.storage.market_store import MarketStore
from data.storage.runtime_store import RuntimeStore

DEFAULT_WORKBOOK = "中国境内ETF全量梳理.xlsx"
STOCK_CATEGORY_SHEET = "股票分类体系"
STOCK_SAMPLE_SHEET = "股票样例分类"
INSTRUMENTS_PATH = PROJECT_ROOT / "config" / "instruments.yaml"

L1_PRIORITY = {
    "宽基": 1,
    "行业": 2,
    "股票": 3,
    "跨境": 4,
    "商品": 5,
    "策略": 6,
    "债券": 7,
}
ETF_L1_ALIASES = {"宽基指数": "宽基"}


@dataclass(frozen=True)
class StockItem:
    symbol: str
    code: str
    exchange: str
    name: str
    category_l1: str
    category_l2: str
    category_l3: str
    reason: str
    sw_industry: str
    priority_l1: int
    priority_l2: int
    priority_l3: int
    sort_order: int
    source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import stock instruments from Excel and backfill raw/qfq daily bars."
    )
    parser.add_argument("--workbook", default=DEFAULT_WORKBOOK)
    parser.add_argument("--start-date", default="1990-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--sleep-seconds", type=float, default=6.5)
    parser.add_argument("--jitter-seconds", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--batch-pause-seconds", type=float, default=60.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--metadata-only", action="store_true")
    parser.add_argument("--force-history", action="store_true")
    return parser.parse_args()


def clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def resolve_workbook(path_text: str) -> Path:
    candidate = Path(path_text)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    if candidate.exists():
        return candidate
    matches = sorted(PROJECT_ROOT.glob("*ETF*梳理*.xlsx"))
    if len(matches) == 1:
        return matches[0]
    raise FileNotFoundError(f"workbook not found: {path_text}")


def code_to_symbol(value: Any) -> tuple[str, str, str] | None:
    text = clean(value).upper()
    match = re.fullmatch(r"(\d{6})\.(SH|SZ|SS)", text)
    if not match:
        return None
    code, suffix = match.groups()
    exchange = "SH" if suffix in {"SH", "SS"} else "SZ"
    internal_suffix = "SS" if exchange == "SH" else "SZ"
    return f"{code}.{internal_suffix}", code, exchange


def is_etf_like(row: pd.Series) -> bool:
    l2 = clean(row.get("二级分类"))
    name = clean(row.get("股票名称"))
    reason = clean(row.get("归类理由"))
    return l2 == "ETF" or "ETF" in name.upper() or "非个股" in reason or "ETF" in reason.upper()


def load_stock_category_order(workbook_path: Path) -> tuple[dict[str, int], dict[tuple[str, str], int]]:
    df = pd.read_excel(workbook_path, sheet_name=STOCK_CATEGORY_SHEET, skiprows=3, dtype=str)
    l2_order: dict[str, int] = {}
    l3_order: dict[tuple[str, str], int] = {}
    for _, row in df.iterrows():
        l1 = clean(row.get("一级分类"))
        l2 = clean(row.get("二级分类"))
        l3 = clean(row.get("三级分类"))
        if l1 != "股票" or not l2:
            continue
        if l2 not in l2_order:
            l2_order[l2] = len(l2_order) + 1
        if l3 and (l2, l3) not in l3_order:
            siblings = [key for key in l3_order if key[0] == l2]
            l3_order[(l2, l3)] = len(siblings) + 1
    return l2_order, l3_order


def load_stock_items(workbook_path: Path) -> list[StockItem]:
    l2_order, l3_order = load_stock_category_order(workbook_path)
    df = pd.read_excel(workbook_path, sheet_name=STOCK_SAMPLE_SHEET, skiprows=3, dtype=str)
    items: list[StockItem] = []
    seen: set[str] = set()
    for _, row in df.iterrows():
        parsed = code_to_symbol(row.get("代码"))
        if parsed is None or is_etf_like(row):
            continue
        symbol, code, exchange = parsed
        if symbol in seen:
            continue
        seen.add(symbol)
        l2 = clean(row.get("二级分类"))
        l3 = clean(row.get("三级分类"))
        sort_order = len(items) + 1
        items.append(
            StockItem(
                symbol=symbol,
                code=code,
                exchange=exchange,
                name=clean(row.get("股票名称")) or symbol,
                category_l1="股票",
                category_l2=l2,
                category_l3=l3,
                reason=clean(row.get("归类理由")),
                sw_industry=clean(row.get("对应申万")),
                priority_l1=L1_PRIORITY["股票"],
                priority_l2=l2_order.get(l2, 9999),
                priority_l3=l3_order.get((l2, l3), 9999),
                sort_order=sort_order,
                source=workbook_path.name,
            )
        )
    return items


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8-sig")) or {}
    return payload if isinstance(payload, dict) else {}


def normalize_etf_l1(value: Any) -> str:
    text = clean(value)
    return ETF_L1_ALIASES.get(text, text)


def adjust_existing_metadata_priorities(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in items:
        copied = dict(item)
        l1 = normalize_etf_l1(copied.get("category_l1") or copied.get("category"))
        if l1:
            copied["category_l1"] = l1
            copied["priority_l1"] = L1_PRIORITY.get(l1, copied.get("priority_l1"))
        out.append(copied)
    return out


def stock_metadata_rows(items: list[StockItem]) -> list[dict[str, Any]]:
    return [
        {
            "symbol": item.symbol,
            "name": item.name,
            "category_l1": item.category_l1,
            "category_l2": item.category_l2,
            "category_l3": item.category_l3,
            "factor_tags": [],
            "region_tag": "A股",
            "priority_l1": item.priority_l1,
            "priority_l2": item.priority_l2,
            "priority_l3": item.priority_l3,
            "sort_order": item.sort_order,
            "source": item.source,
        }
        for item in items
    ]


def build_categories(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    categories: dict[str, dict[str, Any]] = {}
    for item in items:
        l1 = clean(item.get("category_l1"))
        l2 = clean(item.get("category_l2"))
        l3 = clean(item.get("category_l3"))
        if l1:
            categories[l1] = {
                "path": l1,
                "level": 1,
                "name": l1,
                "parent_path": "",
                "priority": L1_PRIORITY.get(l1, item.get("priority_l1")),
            }
        if l1 and l2:
            path = f"{l1}-{l2}"
            categories.setdefault(
                path,
                {
                    "path": path,
                    "level": 2,
                    "name": l2,
                    "parent_path": l1,
                    "priority": item.get("priority_l2"),
                },
            )
        if l1 and l2 and l3:
            parent_path = f"{l1}-{l2}"
            path = f"{parent_path}-{l3}"
            categories.setdefault(
                path,
                {
                    "path": path,
                    "level": 3,
                    "name": l3,
                    "parent_path": parent_path,
                    "priority": item.get("priority_l3"),
                },
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


def update_instruments_yaml(
    existing_metadata: list[dict[str, Any]],
    stock_items: list[StockItem],
    dry_run: bool,
) -> dict[str, Any]:
    payload = load_yaml(INSTRUMENTS_PATH)
    instruments = payload.get("instruments", []) if isinstance(payload, dict) else []
    if not isinstance(instruments, list):
        instruments = []

    existing_by_symbol: dict[str, dict[str, Any]] = {}
    for row in instruments:
        if not isinstance(row, dict):
            continue
        symbol = clean(row.get("symbol")).upper()
        if symbol:
            existing_by_symbol[symbol] = dict(row)

    metadata_by_symbol = {clean(item.get("symbol")).upper(): item for item in existing_metadata}
    stock_metadata_by_symbol = {item.symbol: item for item in stock_items}
    default_risk = float(instruments[0].get("risk_budget_pct", 0.01)) if instruments else 0.01
    default_stop = float(instruments[0].get("stop_atr_mul", 1.5)) if instruments else 1.5

    target_symbols = set(metadata_by_symbol) | set(stock_metadata_by_symbol)
    new_rows: list[dict[str, Any]] = []
    added: list[str] = []

    for symbol in target_symbols:
        row = dict(existing_by_symbol.get(symbol, {}))
        stock_item = stock_metadata_by_symbol.get(symbol)
        metadata = metadata_by_symbol.get(symbol, {})
        is_stock = stock_item is not None
        if not row:
            added.append(symbol)
        row["symbol"] = symbol
        row["name"] = stock_item.name if stock_item else clean(metadata.get("name"))
        row["enabled"] = bool(row.get("enabled", True))
        row["risk_budget_pct"] = float(row.get("risk_budget_pct", default_risk))
        row["stop_atr_mul"] = float(row.get("stop_atr_mul", default_stop))
        row["asset_type"] = "stock" if is_stock else row.get("asset_type", "etf")
        row["category_l1"] = stock_item.category_l1 if stock_item else clean(metadata.get("category_l1"))
        row["category_l2"] = stock_item.category_l2 if stock_item else clean(metadata.get("category_l2"))
        row["category_l3"] = stock_item.category_l3 if stock_item else clean(metadata.get("category_l3"))
        row["priority_l1"] = stock_item.priority_l1 if stock_item else metadata.get("priority_l1")
        row["priority_l2"] = stock_item.priority_l2 if stock_item else metadata.get("priority_l2")
        row["priority_l3"] = stock_item.priority_l3 if stock_item else metadata.get("priority_l3")
        row["sort_order"] = stock_item.sort_order if stock_item else metadata.get("sort_order")
        new_rows.append(row)

    def sort_key(row: dict[str, Any]) -> tuple:
        return (
            int(row.get("priority_l1") or 9999),
            int(row.get("priority_l2") or 9999),
            int(row.get("priority_l3") or 9999),
            int(row.get("sort_order") or 999999),
            str(row.get("symbol") or ""),
        )

    new_rows.sort(key=sort_key)
    removed = [symbol for symbol in existing_by_symbol if symbol not in target_symbols]
    if not dry_run:
        INSTRUMENTS_PATH.write_text(
            yaml.safe_dump(
                {"instruments": new_rows},
                sort_keys=False,
                allow_unicode=True,
                width=120,
            ),
            encoding="utf-8",
        )
    return {"added": added, "removed": removed, "count": len(new_rows)}


def retry_pause_seconds(exc: Exception, base_sleep_seconds: float, attempt: int) -> float:
    match = re.search(r"请\s*(\d+)ms\s*后重试", str(exc))
    if match:
        return int(match.group(1)) / 1000 + 1.0
    return base_sleep_seconds * attempt + random.uniform(0, base_sleep_seconds)


def fetch_with_retries(
    provider: TickFlowProvider,
    item: StockItem,
    start_date: date,
    end_date: date,
    adjust: str,
    max_retries: int,
    sleep_seconds: float,
) -> pd.DataFrame:
    last_error: Exception | None = None
    attempts = max(max_retries, 1)
    for attempt in range(1, attempts + 1):
        try:
            df = provider.fetch_daily_history(item.symbol, start_date, end_date, adjust=adjust)
            if not df.empty:
                df["provider"] = provider.name
            return df
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                pause = retry_pause_seconds(exc, sleep_seconds, attempt)
                print(
                    f"  retry {attempt}/{attempts - 1} for {item.symbol} {adjust} after {pause:.1f}s: {exc}",
                    flush=True,
                )
                time.sleep(pause)
    assert last_error is not None
    raise last_error


def date_span(df: pd.DataFrame) -> tuple[str | None, str | None]:
    if df.empty or "time" not in df.columns:
        return None, None
    ts = pd.to_datetime(df["time"], errors="coerce").dropna()
    if ts.empty:
        return None, None
    return ts.min().date().isoformat(), ts.max().date().isoformat()


def merge_and_save(store: MarketStore, symbol: str, existing: pd.DataFrame, fetched: pd.DataFrame) -> dict:
    merged = pd.concat([existing, fetched], ignore_index=True)
    merged["time"] = pd.to_datetime(merged["time"], errors="coerce")
    merged = (
        merged.dropna(subset=["time"])
        .drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )
    store.save_history(symbol, merged)
    start, end = date_span(merged)
    return {"rows": int(len(merged)), "start": start, "end": end}


def backfill_history(
    stock_items: list[StockItem],
    start_date: date,
    end_date: date,
    sleep_seconds: float,
    jitter_seconds: float,
    batch_size: int,
    batch_pause_seconds: float,
    max_retries: int,
    force_history: bool,
) -> list[dict[str, Any]]:
    provider = TickFlowProvider()
    stores = {"raw": MarketStore(price_mode="raw"), "qfq": MarketStore(price_mode="qfq")}
    adjust_by_mode = {"raw": "none", "qfq": "qfq"}
    results: list[dict[str, Any]] = []
    request_count = 0
    try:
        for item_index, item in enumerate(stock_items, start=1):
            item_result: dict[str, Any] = {
                "symbol": item.symbol,
                "name": item.name,
                "category_l2": item.category_l2,
                "category_l3": item.category_l3,
            }
            for mode in ("raw", "qfq"):
                store = stores[mode]
                existing = store.load_history(item.symbol)
                existing_start, existing_end = date_span(existing)
                if (
                    not force_history
                    and not existing.empty
                    and existing_start is not None
                    and existing_end is not None
                    and existing_end >= end_date.isoformat()
                ):
                    item_result[mode] = {
                        "status": "skipped_complete",
                        "rows": int(len(existing)),
                        "start": existing_start,
                        "end": existing_end,
                    }
                    print(
                        f"[{item_index}/{len(stock_items)}] {item.symbol} {item.name} {mode} skipped rows={len(existing)}",
                        flush=True,
                    )
                    continue

                try:
                    fetched = fetch_with_retries(
                        provider,
                        item,
                        start_date,
                        end_date,
                        adjust_by_mode[mode],
                        max_retries,
                        sleep_seconds,
                    )
                    request_count += 1
                    if fetched.empty:
                        item_result[mode] = {
                            "status": "no_data",
                            "rows": int(len(existing)),
                            "start": existing_start,
                            "end": existing_end,
                        }
                    else:
                        summary = merge_and_save(store, item.symbol, existing, fetched)
                        item_result[mode] = {
                            "status": "updated",
                            **summary,
                            "fetched_rows": int(len(fetched)),
                        }
                    print(
                        f"[{item_index}/{len(stock_items)}] {item.symbol} {item.name} {mode} "
                        f"{item_result[mode]['status']} rows={item_result[mode]['rows']}",
                        flush=True,
                    )
                except Exception as exc:
                    item_result[mode] = {"status": "error", "error": str(exc)}
                    print(f"[{item_index}/{len(stock_items)}] {item.symbol} {item.name} {mode} error: {exc}", flush=True)

                pause = max(sleep_seconds, 0) + random.uniform(0, max(jitter_seconds, 0))
                if pause:
                    time.sleep(pause)
                if batch_size > 0 and request_count > 0 and request_count % batch_size == 0:
                    print(f"Batch pause {batch_pause_seconds:.1f}s", flush=True)
                    time.sleep(max(batch_pause_seconds, 0))
            results.append(item_result)
    finally:
        provider.close()
    return results


def main() -> int:
    args = parse_args()
    workbook_path = resolve_workbook(args.workbook)
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    stock_items = load_stock_items(workbook_path)
    if args.limit and args.limit > 0:
        stock_items = stock_items[: args.limit]
    if not stock_items:
        raise RuntimeError("no valid stock rows found")

    print(f"Loaded {len(stock_items)} stock rows from {workbook_path.name}", flush=True)
    if args.dry_run:
        for item in stock_items[:20]:
            print(f"{item.symbol} {item.name} {item.category_l2}/{item.category_l3}", flush=True)
        return 0

    db = init_db()
    stock_symbols = {item.symbol for item in stock_items}
    existing_metadata = [
        item
        for item in adjust_existing_metadata_priorities(db.list_instrument_metadata())
        if clean(item.get("symbol")).upper() not in stock_symbols
    ]
    combined_metadata = existing_metadata + stock_metadata_rows(stock_items)
    category_count = db.replace_instrument_categories(build_categories(combined_metadata))
    metadata_count = db.save_instrument_metadata(combined_metadata)
    instruments_result = update_instruments_yaml(existing_metadata, stock_items, dry_run=False)

    history_results: list[dict[str, Any]] = []
    if not args.metadata_only:
        history_results = backfill_history(
            stock_items,
            start_date,
            end_date,
            args.sleep_seconds,
            args.jitter_seconds,
            args.batch_size,
            args.batch_pause_seconds,
            args.max_retries,
            args.force_history,
        )

    counts = {
        "stocks": len(stock_items),
        "metadata_rows_saved": metadata_count,
        "categories": category_count,
        "instruments": instruments_result["count"],
        "history_errors": sum(
            1
            for row in history_results
            for mode in ("raw", "qfq")
            if isinstance(row.get(mode), dict) and row[mode].get("status") == "error"
        ),
        "history_no_data": sum(
            1
            for row in history_results
            for mode in ("raw", "qfq")
            if isinstance(row.get(mode), dict) and row[mode].get("status") == "no_data"
        ),
    }
    report = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "workbook": str(workbook_path),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "counts": counts,
        "instruments": instruments_result,
        "history_results": history_results,
    }
    runtime_store = RuntimeStore()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = runtime_store.write_json(f"advice/stock_pool_import_{stamp}.json", report)
    runtime_store.write_json("advice/stock_pool_import_latest.json", report)
    print(json.dumps({**counts, "report_path": str(report_path)}, ensure_ascii=False), flush=True)
    return 0 if counts["history_errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
