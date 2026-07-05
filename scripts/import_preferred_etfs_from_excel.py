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

DEFAULT_WORKBOOK = "\u4e2d\u56fd\u5883\u5185ETF\u5168\u91cf\u68b3\u7406.xlsx"
DEFAULT_SHEET = "\u4f18\u9009ETF"

CODE_COL = "ETF\u4ee3\u7801"
NAME_COL = "ETF\u540d\u79f0"
CATEGORY_COL = "\u6240\u5c5e\u5206\u7c7b"
INDEX_COL = "\u8ddf\u8e2a\u6307\u6570"
PEER_COUNT_COL = "\u540c\u6307\u6570ETF\u6570"
SIZE_THRESHOLD_COL = "\u89c4\u6a21\u95e8\u69db(\u4ebf)"
FUND_SIZE_COL = "\u57fa\u91d1\u89c4\u6a21(\u4ebf\u5143)"
MANAGEMENT_FEE_COL = "\u7ba1\u7406\u8d39\u7387(%)"
CUSTODY_FEE_COL = "\u6258\u7ba1\u8d39\u7387(%)"
TOTAL_FEE_COL = "\u5408\u8ba1\u8d39\u7387(%)"

METADATA_PATH = PROJECT_ROOT / "data" / "market" / "etf" / "metadata.json"
INSTRUMENTS_PATH = PROJECT_ROOT / "config" / "instruments.yaml"


@dataclass(frozen=True)
class EtfItem:
    symbol: str
    code: str
    exchange: str
    name: str
    category: str | None
    tracking_index: str | None
    peer_count: float | None
    size_threshold_billion: float | None
    fund_size_billion: float | None
    management_fee_pct: float | None
    custody_fee_pct: float | None
    total_fee_pct: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import preferred ETF pool from Excel and backfill daily bars."
    )
    parser.add_argument("--workbook", default=DEFAULT_WORKBOOK)
    parser.add_argument("--sheet", default=DEFAULT_SHEET)
    parser.add_argument("--start-date", default="1990-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--adjust", choices=["qfq", "hfq", "none"], default="qfq")
    parser.add_argument(
        "--price-mode",
        choices=["qfq", "raw"],
        default="qfq",
        help="Target local price table. Use raw with --adjust none.",
    )
    parser.add_argument("--sleep-seconds", type=float, default=6.5)
    parser.add_argument("--jitter-seconds", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--batch-pause-seconds", type=float, default=60.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--rebuild-market-data",
        action="store_true",
        help="Delete all rows from market_data before importing the Excel pool.",
    )
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def read_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig") as f:
        payload = json.load(f)
    return payload if isinstance(payload, dict) else {}


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text == "" or text.lower() in {"nan", "none", "-"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none"}:
        return None
    return text


def code_to_symbol(raw_code: Any) -> tuple[str, str, str]:
    text = str(raw_code).strip().upper()
    if "." in text:
        code, suffix = text.split(".", 1)
    else:
        code = "".join(ch for ch in text if ch.isdigit())
        suffix = "SH" if code.startswith(("5", "6")) else "SZ"

    code = "".join(ch for ch in code if ch.isdigit())
    if len(code) != 6 or suffix not in {"SH", "SZ", "SS"}:
        raise ValueError(f"invalid ETF code: {raw_code}")

    exchange = "SH" if suffix in {"SH", "SS"} else "SZ"
    internal_suffix = "SS" if exchange == "SH" else "SZ"
    return f"{code}.{internal_suffix}", code, exchange


def resolve_workbook(path_text: str) -> Path:
    candidate = Path(path_text)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    if candidate.exists():
        return candidate

    matches = sorted(PROJECT_ROOT.glob("*ETF*\u68b3\u7406*.xlsx"))
    if len(matches) == 1:
        return matches[0]
    raise FileNotFoundError(f"workbook not found: {path_text}")


def load_preferred_etfs(workbook_path: Path, sheet: str) -> list[EtfItem]:
    df = pd.read_excel(
        workbook_path,
        sheet_name=sheet,
        skiprows=10,
        usecols="B:L",
        dtype=str,
    )
    required = {CODE_COL, NAME_COL}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"missing expected Excel columns: {sorted(missing)}")

    items: list[EtfItem] = []
    seen: set[str] = set()
    for _, row in df.iterrows():
        raw_code = clean_text(row.get(CODE_COL))
        if raw_code is None:
            continue
        try:
            symbol, code, exchange = code_to_symbol(raw_code)
        except ValueError:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        items.append(
            EtfItem(
                symbol=symbol,
                code=code,
                exchange=exchange,
                name=clean_text(row.get(NAME_COL)) or symbol,
                category=clean_text(row.get(CATEGORY_COL)),
                tracking_index=clean_text(row.get(INDEX_COL)),
                peer_count=to_float(row.get(PEER_COUNT_COL)),
                size_threshold_billion=to_float(row.get(SIZE_THRESHOLD_COL)),
                fund_size_billion=to_float(row.get(FUND_SIZE_COL)),
                management_fee_pct=to_float(row.get(MANAGEMENT_FEE_COL)),
                custody_fee_pct=to_float(row.get(CUSTODY_FEE_COL)),
                total_fee_pct=to_float(row.get(TOTAL_FEE_COL)),
            )
        )
    return items


def date_span(df: pd.DataFrame) -> tuple[str | None, str | None]:
    if df.empty or "time" not in df.columns:
        return None, None
    ts = pd.to_datetime(df["time"], errors="coerce").dropna()
    if ts.empty:
        return None, None
    return ts.min().date().isoformat(), ts.max().date().isoformat()


def should_skip_complete(
    metadata: dict[str, Any],
    item: EtfItem,
    price_mode: str,
    start_date: date,
    end_date: date,
) -> bool:
    entry = metadata.get(item.symbol)
    if not isinstance(entry, dict):
        return False
    prefix = f"history_{price_mode}"
    if entry.get(f"{prefix}_requested_start") != start_date.isoformat():
        return False
    requested_end = str(entry.get(f"{prefix}_requested_end") or "")
    if requested_end < end_date.isoformat():
        return False
    return int(entry.get(f"{prefix}_rows") or 0) > 0


def fetch_with_retries(
    provider: TickFlowProvider,
    item: EtfItem,
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
                    f"  retry {attempt}/{attempts - 1} for {item.symbol} after {pause:.1f}s: {exc}",
                    flush=True,
                )
                time.sleep(pause)
    assert last_error is not None
    raise last_error


def retry_pause_seconds(exc: Exception, base_sleep_seconds: float, attempt: int) -> float:
    message = str(exc)
    match = re.search(r"\u8bf7\s*(\d+)ms\s*\u540e\u91cd\u8bd5", message)
    if match:
        return int(match.group(1)) / 1000 + 1.0
    return base_sleep_seconds * attempt + random.uniform(0, base_sleep_seconds)


def update_instruments_yaml(items: list[EtfItem], dry_run: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if INSTRUMENTS_PATH.exists():
        payload = yaml.safe_load(INSTRUMENTS_PATH.read_text(encoding="utf-8-sig")) or {}
    instruments = payload.get("instruments", []) if isinstance(payload, dict) else []
    if not isinstance(instruments, list):
        instruments = []

    existing_by_symbol: dict[str, dict[str, Any]] = {}
    for row in instruments:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol:
            existing_by_symbol[symbol] = dict(row)

    default_risk = 0.01
    default_stop = 1.5
    if instruments and isinstance(instruments[0], dict):
        default_risk = float(instruments[0].get("risk_budget_pct", default_risk))
        default_stop = float(instruments[0].get("stop_atr_mul", default_stop))

    target_symbols = [item.symbol for item in items]
    target_symbol_set = set(target_symbols)
    new_rows: list[dict[str, Any]] = []
    added: list[str] = []

    for item in items:
        row = existing_by_symbol.get(item.symbol)
        if row is None:
            row = {
                "symbol": item.symbol,
                "name": item.name,
                "enabled": True,
                "risk_budget_pct": default_risk,
                "stop_atr_mul": default_stop,
            }
            added.append(item.symbol)
        else:
            row["symbol"] = item.symbol
            row["name"] = item.name
            row["enabled"] = bool(row.get("enabled", True))
            row["risk_budget_pct"] = float(row.get("risk_budget_pct", default_risk))
            row["stop_atr_mul"] = float(row.get("stop_atr_mul", default_stop))
        new_rows.append(row)

    removed = [symbol for symbol in existing_by_symbol if symbol not in target_symbol_set]
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


def metadata_entry(
    item: EtfItem,
    price_mode: str,
    status: str,
    requested_start: date,
    requested_end: date,
    rows: int,
    history_start: str | None,
    history_end: str | None,
    error: str | None = None,
) -> dict[str, Any]:
    prefix = f"history_{price_mode}"
    entry: dict[str, Any] = {
        "symbol": item.symbol,
        "code": item.code,
        "exchange": item.exchange,
        "name": item.name,
        "category": item.category,
        "tracking_index": item.tracking_index,
        "peer_count": item.peer_count,
        "size_threshold_billion": item.size_threshold_billion,
        "fund_size_billion": item.fund_size_billion,
        "management_fee_pct": item.management_fee_pct,
        "custody_fee_pct": item.custody_fee_pct,
        "total_fee_pct": item.total_fee_pct,
        f"{prefix}_status": status,
        f"{prefix}_rows": rows,
        f"{prefix}_start": history_start,
        f"{prefix}_end": history_end,
        f"{prefix}_requested_start": requested_start.isoformat(),
        f"{prefix}_requested_end": requested_end.isoformat(),
        f"{prefix}_path": f"sqlite/{price_mode}/{item.symbol}",
        "metadata_updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if price_mode == "qfq":
        entry.update(
            {
                "history_status": status,
                "history_rows": rows,
                "history_start": history_start,
                "history_end": history_end,
                "history_requested_start": requested_start.isoformat(),
                "history_requested_end": requested_end.isoformat(),
                "history_path": f"sqlite/{price_mode}/{item.symbol}",
            }
        )
    if error:
        entry[f"{prefix}_error"] = error
    return entry


def main() -> int:
    args = parse_args()
    workbook_path = resolve_workbook(args.workbook)
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    items = load_preferred_etfs(workbook_path, args.sheet)
    if args.limit and args.limit > 0:
        items = items[: args.limit]
    if not items:
        raise RuntimeError("no ETF rows found in workbook")

    print(f"Loaded {len(items)} ETF rows from {workbook_path.name} / {args.sheet}", flush=True)
    if args.price_mode == "raw" and args.adjust != "none":
        raise ValueError("--price-mode raw must be used with --adjust none")

    if args.dry_run:
        for item in items[:10]:
            print(f"{item.symbol} {item.name} {item.category or ''}", flush=True)
        print("Dry run finished; no files were written.", flush=True)
        return 0

    db = init_db()
    store = MarketStore(db, price_mode=args.price_mode)
    provider = TickFlowProvider()
    runtime_store = RuntimeStore()

    old_metadata = read_json_dict(METADATA_PATH)
    if args.rebuild_market_data:
        deleted_rows = db.clear_market_data(price_mode=args.price_mode)
        old_metadata = {}
        print(f"Cleared {args.price_mode} market data rows={deleted_rows}", flush=True)

    new_metadata: dict[str, Any] = {}
    results: list[dict[str, Any]] = []

    try:
        for index, item in enumerate(items, start=1):
            request_made = False
            prefix = f"[{index}/{len(items)}] {item.symbol} {item.name}"
            try:
                if not args.force and should_skip_complete(
                    old_metadata,
                    item,
                    args.price_mode,
                    start_date,
                    end_date,
                ):
                    summary = db.get_market_data_summary(item.symbol, price_mode=args.price_mode)
                    entry = metadata_entry(
                        item,
                        args.price_mode,
                        "skipped_complete",
                        start_date,
                        end_date,
                        int(summary["rows"]),
                        summary["start"],
                        summary["end"],
                    )
                    previous = old_metadata.get(item.symbol, {})
                    new_metadata[item.symbol] = {
                        **(previous if isinstance(previous, dict) else {}),
                        **entry,
                    }
                    results.append(entry)
                    print(
                        f"{prefix} skipped rows={entry[f'history_{args.price_mode}_rows']} "
                        f"range={entry[f'history_{args.price_mode}_start']}.."
                        f"{entry[f'history_{args.price_mode}_end']}",
                        flush=True,
                    )
                else:
                    request_made = True
                    existing = store.load_history(item.symbol)
                    rows_before = int(len(existing))
                    fetched = fetch_with_retries(
                        provider,
                        item,
                        start_date,
                        end_date,
                        args.adjust,
                        args.max_retries,
                        args.sleep_seconds,
                    )
                    if fetched.empty:
                        existing_start, existing_end = date_span(existing)
                        entry = metadata_entry(
                            item,
                            args.price_mode,
                            "no_data",
                            start_date,
                            end_date,
                            rows_before,
                            existing_start,
                            existing_end,
                        )
                        print(f"{prefix} no_data existing_rows={rows_before}", flush=True)
                    else:
                        merged = pd.concat([existing, fetched], ignore_index=True)
                        merged["time"] = pd.to_datetime(merged["time"], errors="coerce")
                        merged = (
                            merged.dropna(subset=["time"])
                            .drop_duplicates(subset=["time"])
                            .sort_values("time")
                            .reset_index(drop=True)
                        )
                        store.save_history(item.symbol, merged)
                        history_start, history_end = date_span(merged)
                        entry = metadata_entry(
                            item,
                            args.price_mode,
                            "updated",
                            start_date,
                            end_date,
                            int(len(merged)),
                            history_start,
                            history_end,
                        )
                        entry["rows_before"] = rows_before
                        entry["fetched_rows"] = int(len(fetched))
                        entry["added_rows"] = int(len(merged)) - rows_before
                        print(
                            f"{prefix} updated rows={entry[f'history_{args.price_mode}_rows']} "
                            f"fetched={entry['fetched_rows']} "
                            f"range={history_start}..{history_end}",
                            flush=True,
                        )
                    previous = old_metadata.get(item.symbol, {})
                    new_metadata[item.symbol] = {
                        **(previous if isinstance(previous, dict) else {}),
                        **entry,
                    }
                    results.append(entry)
            except Exception as exc:
                entry = metadata_entry(
                    item,
                    args.price_mode,
                    "error",
                    start_date,
                    end_date,
                    0,
                    None,
                    None,
                    error=str(exc),
                )
                previous = old_metadata.get(item.symbol, {})
                new_metadata[item.symbol] = {
                    **(previous if isinstance(previous, dict) else {}),
                    **entry,
                }
                results.append(entry)
                print(f"{prefix} error: {exc}", flush=True)

            if request_made and index < len(items):
                pause = max(args.sleep_seconds, 0) + random.uniform(0, max(args.jitter_seconds, 0))
                if pause:
                    time.sleep(pause)
                if args.batch_size > 0 and index % args.batch_size == 0:
                    print(f"Batch pause {args.batch_pause_seconds:.1f}s", flush=True)
                    time.sleep(max(args.batch_pause_seconds, 0))

    finally:
        provider.close()

    METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    METADATA_PATH.write_text(
        json.dumps(new_metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    instruments_result = update_instruments_yaml(items, dry_run=False)

    report = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "workbook": str(workbook_path),
        "sheet": args.sheet,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "adjust": args.adjust,
        "price_mode": args.price_mode,
        "metadata_path": str(METADATA_PATH),
        "instruments_path": str(INSTRUMENTS_PATH),
        "instruments": instruments_result,
        "counts": {
            "total": len(results),
            "updated": sum(
                1 for row in results if row[f"history_{args.price_mode}_status"] == "updated"
            ),
            "skipped_complete": sum(
                1
                for row in results
                if row[f"history_{args.price_mode}_status"] == "skipped_complete"
            ),
            "no_data": sum(
                1 for row in results if row[f"history_{args.price_mode}_status"] == "no_data"
            ),
            "error": sum(
                1 for row in results if row[f"history_{args.price_mode}_status"] == "error"
            ),
        },
        "results": results,
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = runtime_store.write_json(f"advice/preferred_etf_import_{stamp}.json", report)
    runtime_store.write_json("advice/preferred_etf_import_latest.json", report)
    print(json.dumps({**report["counts"], "report_path": str(report_path)}, ensure_ascii=False), flush=True)
    return 0 if report["counts"]["error"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
