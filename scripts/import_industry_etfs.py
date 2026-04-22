from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from data.provider_utils import normalize_symbol
from data.service import DataService

TARGET_ETFS = [
    {"code": "512800", "name": "银行"},
    {"code": "512000", "name": "券商"},
    {"code": "512200", "name": "房地产"},
    {"code": "159928", "name": "消费"},
    {"code": "512690", "name": "酒"},
    {"code": "512010", "name": "医药"},
    {"code": "512290", "name": "生物医药"},
    {"code": "512760", "name": "芯片"},
    {"code": "515260", "name": "电子"},
    {"code": "515050", "name": "5G"},
    {"code": "159869", "name": "游戏"},
    {"code": "515030", "name": "新能源车"},
    {"code": "515790", "name": "光伏"},
    {"code": "159755", "name": "电池"},
    {"code": "515220", "name": "煤炭"},
    {"code": "512400", "name": "有色金属"},
    {"code": "515210", "name": "钢铁"},
    {"code": "512660", "name": "军工"},
    {"code": "516950", "name": "基建50"},
    {"code": "159825", "name": "农业"},
]

INDUSTRY_ETF_CODES = [item["code"] for item in TARGET_ETFS]
PREFERRED_NAME_BY_CODE = {item["code"]: item["name"] for item in TARGET_ETFS}

METADATA_PATH = PROJECT_ROOT / "data" / "market" / "etf" / "metadata.json"
INSTRUMENTS_PATH = PROJECT_ROOT / "config" / "instruments.yaml"
STRATEGY_PATH = PROJECT_ROOT / "config" / "strategy.yaml"


def code_to_symbol(code: str) -> str:
    text = str(code).strip().upper()
    if text.endswith(".SH"):
        return text[:-3] + ".SS"
    if text.endswith((".SS", ".SZ")):
        return text

    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        raise ValueError(f"invalid ETF code: {code}")

    if digits.startswith(("5", "6")):
        suffix = ".SS"
    else:
        suffix = ".SZ"
    return f"{digits}{suffix}"


def load_strategy_defaults() -> tuple[date, str]:
    if not STRATEGY_PATH.exists():
        return date(2015, 1, 1), "qfq"

    payload = yaml.safe_load(STRATEGY_PATH.read_text(encoding="utf-8")) or {}
    strategy_cfg = payload.get("strategy", {}) if isinstance(payload, dict) else {}
    start_text = str(strategy_cfg.get("backtest_start_primary", "2015-01-01"))
    adjust = str(strategy_cfg.get("adjust", "qfq"))
    start_date = datetime.strptime(start_text, "%Y-%m-%d").date()
    return start_date, adjust


def load_json_dict(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig") as f:
        content = json.load(f)
    if isinstance(content, dict):
        return content
    return {}


def update_instruments_yaml(symbol_names: dict[str, str | None]) -> tuple[list[str], list[str]]:
    payload = {}
    if INSTRUMENTS_PATH.exists():
        payload = yaml.safe_load(INSTRUMENTS_PATH.read_text(encoding="utf-8")) or {}

    instruments = payload.get("instruments", []) if isinstance(payload, dict) else []
    if not isinstance(instruments, list):
        instruments = []

    existing_by_symbol: dict[str, dict] = {}
    for item in instruments:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol:
            existing_by_symbol[symbol] = item

    default_risk = 0.01
    default_stop = 1.5
    if instruments and isinstance(instruments[0], dict):
        default_risk = float(instruments[0].get("risk_budget_pct", default_risk))
        default_stop = float(instruments[0].get("stop_atr_mul", default_stop))

    target_symbols = list(symbol_names.keys())
    target_symbol_set = set(target_symbols)
    added_symbols: list[str] = []
    normalized_instruments: list[dict] = []
    for symbol in target_symbols:
        name = symbol_names.get(symbol)
        row = existing_by_symbol.get(symbol)
        if row is None:
            new_row = {
                "symbol": symbol,
                "name": name or "",
                "enabled": True,
                "risk_budget_pct": default_risk,
                "stop_atr_mul": default_stop,
            }
            normalized_instruments.append(new_row)
            added_symbols.append(symbol)
            continue

        copied = dict(row)
        copied["symbol"] = symbol
        copied["name"] = name or str(copied.get("name", "") or "")
        copied["enabled"] = bool(copied.get("enabled", True))
        copied["risk_budget_pct"] = float(copied.get("risk_budget_pct", default_risk))
        copied["stop_atr_mul"] = float(copied.get("stop_atr_mul", default_stop))
        normalized_instruments.append(copied)

    removed_symbols = [symbol for symbol in existing_by_symbol.keys() if symbol not in target_symbol_set]

    INSTRUMENTS_PATH.write_text(
        yaml.safe_dump({"instruments": normalized_instruments}, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return added_symbols, removed_symbols


def main() -> None:
    service = DataService(provider_priority=["efinance", "akshare"])
    start_date, adjust = load_strategy_defaults()
    today = date.today()

    symbol_list = []
    for raw_code in INDUSTRY_ETF_CODES:
        symbol = code_to_symbol(raw_code)
        if symbol not in symbol_list:
            symbol_list.append(symbol)

    metadata = load_json_dict(METADATA_PATH)
    now_iso = datetime.now().isoformat()

    results: list[dict] = []
    new_metadata: dict[str, dict] = {}
    symbol_names: dict[str, str | None] = {}
    for symbol in symbol_list:
        try:
            existing = service.market_store.load_history(symbol)
            if existing.empty:
                history_result = service.ensure_daily_history(symbol, start_date=start_date, end_date=today, adjust=adjust)
            else:
                history_result = {
                    "symbol": symbol,
                    "status": "skipped_existing",
                    "rows": int(len(existing)),
                    "path": str(service.market_store.path_for(symbol)),
                }

            name_info = service.fetch_instrument_name(symbol)
            new_name = str(name_info.get("name", "") or "").strip()
            prev_entry = metadata.get(symbol, {}) if isinstance(metadata.get(symbol), dict) else {}
            prev_name = str(prev_entry.get("name", "") or "").strip()
            code = normalize_symbol(symbol)
            preferred_name = str(PREFERRED_NAME_BY_CODE.get(code, "") or "").strip()
            chosen_name = preferred_name or new_name or prev_name or None
            symbol_names[symbol] = chosen_name

            new_metadata[symbol] = {
                "symbol": symbol,
                "code": code,
                "exchange": "SH" if symbol.endswith(".SS") else "SZ",
                "name": chosen_name,
                "name_provider": name_info.get("provider"),
                "name_updated_at": now_iso,
                "history_status": history_result.get("status"),
                "history_rows": history_result.get("rows"),
                "history_path": history_result.get("path"),
            }

            results.append(
                {
                    "symbol": symbol,
                    "name": chosen_name,
                    "history": history_result,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "symbol": symbol,
                    "name": None,
                    "history": {"symbol": symbol, "status": "error"},
                    "error": str(exc),
                }
            )

    METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with METADATA_PATH.open("w", encoding="utf-8") as f:
        json.dump(new_metadata, f, ensure_ascii=False, indent=2)

    added_symbols, removed_symbols = update_instruments_yaml(symbol_names)

    report = {
        "ts": now_iso,
        "start_date": start_date.isoformat(),
        "end_date": today.isoformat(),
        "metadata_path": str(METADATA_PATH),
        "added_to_instruments": added_symbols,
        "removed_from_instruments": removed_symbols,
        "results": results,
    }

    report_path = service.runtime_store.write_json(f"advice/industry_etf_import_{today.isoformat()}.json", report)
    report["report_path"] = str(report_path)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
