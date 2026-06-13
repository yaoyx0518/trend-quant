from __future__ import annotations

from datetime import datetime, time
import json
from pathlib import Path
from typing import Any


DEFAULT_CALC_LOG_PATH = "logs/calc/calc.jsonl"


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compact_value(value: Any) -> Any:
    number = _as_float(value)
    if number is not None:
        return round(number, 6)
    if isinstance(value, list):
        return [_compact_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _compact_value(v) for k, v in value.items()}
    return value


def _field(details: dict, key: str) -> Any:
    return _compact_value(details.get(key))


def build_calc_chain(payload: dict) -> list[dict]:
    details = payload.get("calc_details", {})
    if not isinstance(details, dict):
        details = {}

    chain = [
        {
            "id": "market",
            "title": "行情输入",
            "inputs": [],
            "outputs": [
                {"key": "price", "label": "收盘价", "value": _field(details, "price")},
                {"key": "ma_mid", "label": "中期均线", "value": _field(details, "ma_mid")},
                {"key": "atr", "label": "ATR", "value": _field(details, "atr")},
                {"key": "current_volume", "label": "成交量", "value": _field(details, "current_volume")},
                {"key": "vol_ma", "label": "成交量均值", "value": _field(details, "vol_ma")},
            ],
            "formula": "价格、均线、ATR、成交量作为后续趋势与风控输入",
        },
        {
            "id": "bias",
            "title": "价格偏离",
            "inputs": ["price", "ma_short/ma_mid/ma_long", "atr"],
            "outputs": [
                {"key": "bias_short", "label": "短期偏离", "value": _field(details, "bias_short")},
                {"key": "bias_mid", "label": "中期偏离", "value": _field(details, "bias_mid")},
                {"key": "bias_long", "label": "长期偏离", "value": _field(details, "bias_long")},
                {"key": "bias_mix", "label": "偏离加权", "value": _field(details, "bias_mix")},
                {"key": "norm_bias", "label": "偏离归一", "value": _field(details, "norm_bias")},
            ],
            "formula": "各周期价格偏离以 ATR 标准化，再加权并归一",
        },
        {
            "id": "slope",
            "title": "均线斜率",
            "inputs": ["ema_short", "ema_mid", "ema_long", "atr"],
            "outputs": [
                {"key": "slope_short", "label": "短期斜率", "value": _field(details, "slope_short")},
                {"key": "slope_mid", "label": "中期斜率", "value": _field(details, "slope_mid")},
                {"key": "slope_long", "label": "长期斜率", "value": _field(details, "slope_long")},
                {"key": "slope_mix", "label": "斜率加权", "value": _field(details, "slope_mix")},
                {"key": "norm_slope", "label": "斜率归一", "value": _field(details, "norm_slope")},
            ],
            "formula": "EMA 斜率以 ATR 和周期长度标准化，再加权并归一",
        },
        {
            "id": "direction",
            "title": "方向强度",
            "inputs": ["norm_bias", "norm_slope"],
            "outputs": [
                {
                    "key": "price_direction",
                    "label": "价格方向分",
                    "value": _compact_value(payload.get("price_direction")),
                }
            ],
            "formula": "偏离归一值与斜率归一值加权合成价格方向",
        },
        {
            "id": "confidence",
            "title": "置信度",
            "inputs": ["current_volume", "vol_ma", "er"],
            "outputs": [
                {"key": "vol_ratio", "label": "量比", "value": _field(details, "vol_ratio")},
                {"key": "volume_factor", "label": "量能因子", "value": _field(details, "volume_factor")},
                {"key": "er", "label": "效率比", "value": _field(details, "er")},
                {
                    "key": "confidence",
                    "label": "置信度",
                    "value": _compact_value(payload.get("confidence")),
                },
            ],
            "formula": "量能因子与效率比按权重合成为信号置信度",
        },
        {
            "id": "score",
            "title": "趋势评分",
            "inputs": ["price_direction", "confidence", "prev_score"],
            "outputs": [
                {"key": "prev_prev_score", "label": "前前次分数", "value": _field(details, "prev_prev_score")},
                {"key": "prev_score", "label": "前次分数", "value": _field(details, "prev_score")},
                {
                    "key": "trend_score",
                    "label": "趋势分",
                    "value": _compact_value(payload.get("trend_score")),
                },
            ],
            "formula": "价格方向分乘以置信度，并裁剪到 [-100, 100]",
        },
        {
            "id": "risk",
            "title": "仓位与风控",
            "inputs": ["position_qty", "sellable_qty", "atr", "stop_price"],
            "outputs": [
                {"key": "position_qty", "label": "持仓", "value": _field(details, "position_qty")},
                {"key": "sellable_qty", "label": "可卖", "value": _field(details, "sellable_qty")},
                {"key": "hard_stop_price", "label": "硬止损", "value": _field(details, "hard_stop_price")},
                {
                    "key": "chandelier_stop_price",
                    "label": "吊灯止损",
                    "value": _field(details, "chandelier_stop_price"),
                },
                {
                    "key": "exit_decisions",
                    "label": "退出判断",
                    "value": _compact_value(details.get("exit_decisions", [])),
                },
            ],
            "formula": "当前仓位、止损价和退出规则决定是否触发卖出或 T+1 阻断",
        },
        {
            "id": "decision",
            "title": "交易决策",
            "inputs": ["trend_score", "price", "ma_mid", "entry_threshold", "risk"],
            "outputs": [
                {"key": "entry_threshold_min", "label": "入场下限", "value": _field(details, "entry_threshold_min")},
                {"key": "entry_threshold_max", "label": "入场上限", "value": _field(details, "entry_threshold_max")},
                {"key": "entry_decision", "label": "入场判断", "value": _compact_value(details.get("entry_decision", {}))},
                {"key": "action", "label": "动作", "value": str(payload.get("action", ""))},
                {"key": "level", "label": "级别", "value": str(payload.get("level", ""))},
                {"key": "reason", "label": "原因", "value": str(payload.get("reason", ""))},
            ],
            "formula": "评分窗口、价格位置、过滤器和风控结果共同决定最终动作",
        },
    ]

    return [
        {
            **stage,
            "outputs": [item for item in stage["outputs"] if item.get("value") not in (None, "", [])],
        }
        for stage in chain
    ]


def normalize_calc_record(payload: dict, line_no: int | None = None) -> dict:
    details = payload.get("calc_details", {})
    if not isinstance(details, dict):
        details = {}
    symbol = str(payload.get("symbol", "") or "").strip().upper()
    ts = str(payload.get("ts", "") or "")
    chain = payload.get("calc_chain")
    if not isinstance(chain, list):
        chain = build_calc_chain(payload)

    symbol_config = payload.get("symbol_config") or {}
    if not isinstance(symbol_config, dict):
        symbol_config = {}

    return {
        "line_no": line_no,
        "ts": ts,
        "date": ts[:10] if len(ts) >= 10 else "",
        "symbol": symbol,
        "name": str(symbol_config.get("name", "") or ""),
        "trigger": str(payload.get("trigger", "") or ""),
        "source": str(payload.get("source", payload.get("trigger", "")) or ""),
        "run_id": str(payload.get("run_id", "") or ""),
        "trade_day": str(payload.get("trade_day", "") or ""),
        "action": str(payload.get("action", "") or ""),
        "level": str(payload.get("level", "") or ""),
        "reason": str(payload.get("reason", "") or ""),
        "ok": bool(payload.get("ok", False)),
        "trend_score": _compact_value(payload.get("trend_score")),
        "price_direction": _compact_value(payload.get("price_direction")),
        "confidence": _compact_value(payload.get("confidence")),
        "price": _field(details, "price"),
        "ma_mid": _field(details, "ma_mid"),
        "atr": _field(details, "atr"),
        "entry_candidate": bool(payload.get("entry_candidate", False)),
        "entry_passed": payload.get("entry_passed"),
        "calc_chain": chain,
        "raw": payload,
    }


def _parse_query_datetime(value: str | None, *, is_end: bool = False) -> datetime | None:
    text = str(value or "").strip()
    if text == "":
        return None
    try:
        if len(text) == 10:
            base = datetime.fromisoformat(text)
            return datetime.combine(base.date(), time.max if is_end else time.min)
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _record_datetime(record: dict) -> datetime | None:
    return _parse_query_datetime(str(record.get("ts", "") or ""))


def iter_calc_records(file_path: str = DEFAULT_CALC_LOG_PATH) -> list[dict]:
    path = Path(file_path)
    if not path.exists():
        return []

    records: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                records.append(normalize_calc_record(payload, line_no=line_no))
    return records


def iter_calc_payloads(file_path: str = DEFAULT_CALC_LOG_PATH) -> list[dict]:
    path = Path(file_path)
    if not path.exists():
        return []

    payloads: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                payloads.append(payload)
    return payloads


def list_calc_symbols(file_path: str = DEFAULT_CALC_LOG_PATH) -> list[dict]:
    symbol_map: dict[str, str] = {}
    for payload in iter_calc_payloads(file_path=file_path):
        symbol = str(payload.get("symbol", "") or "").strip().upper()
        if not symbol:
            continue
        symbol_config = payload.get("symbol_config") or {}
        name = str(symbol_config.get("name", "") or "") if isinstance(symbol_config, dict) else ""
        symbol_map.setdefault(symbol, name)
    return [
        {"symbol": symbol, "name": name}
        for symbol, name in sorted(symbol_map.items(), key=lambda item: item[0])
    ]


def list_calc_runs(file_path: str = DEFAULT_CALC_LOG_PATH, source: str = "") -> list[dict]:
    normalized_source = str(source or "").strip().lower()
    seen: dict[str, dict] = {}
    for payload in iter_calc_payloads(file_path=file_path):
        payload_source = str(payload.get("source", payload.get("trigger", "")) or "")
        if normalized_source and payload_source.lower() != normalized_source:
            continue
        run_id = str(payload.get("run_id", "") or "").strip()
        if not run_id:
            continue
        ts = str(payload.get("trade_day", "") or payload.get("ts", "") or "")
        date_text = ts[:10] if len(ts) >= 10 else ""
        item = seen.setdefault(
            run_id,
            {
                "run_id": run_id,
                "source": payload_source,
                "start": date_text,
                "end": date_text,
                "count": 0,
            },
        )
        if date_text:
            item["start"] = min(str(item.get("start") or date_text), date_text)
            item["end"] = max(str(item.get("end") or date_text), date_text)
        item["count"] = int(item.get("count", 0) or 0) + 1
    return sorted(seen.values(), key=lambda item: str(item.get("run_id", "")), reverse=True)


def query_calc_records(
    *,
    symbol: str = "",
    start: str = "",
    end: str = "",
    source: str = "",
    run_id: str = "",
    limit: int = 0,
    file_path: str = DEFAULT_CALC_LOG_PATH,
) -> dict:
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_source = str(source or "").strip().lower()
    normalized_run_id = str(run_id or "").strip()
    start_dt = _parse_query_datetime(start)
    end_dt = _parse_query_datetime(end, is_end=True)

    records = iter_calc_records(file_path=file_path)
    symbols = sorted({record["symbol"] for record in records if record.get("symbol")})
    filtered: list[dict] = []
    for record in records:
        if normalized_symbol and record.get("symbol") != normalized_symbol:
            continue
        if normalized_source and str(record.get("source", "") or "").lower() != normalized_source:
            continue
        if normalized_run_id and str(record.get("run_id", "") or "") != normalized_run_id:
            continue
        record_dt = _record_datetime(record)
        if start_dt and (record_dt is None or record_dt < start_dt):
            continue
        if end_dt and (record_dt is None or record_dt > end_dt):
            continue
        filtered.append(record)

    total = len(filtered)
    if limit > 0:
        filtered = filtered[-limit:]

    return {
        "items": filtered,
        "symbols": symbols,
        "meta": {
            "symbol": normalized_symbol,
            "source": normalized_source,
            "run_id": normalized_run_id,
            "start": start,
            "end": end,
            "limit": limit,
            "total": total,
            "returned": len(filtered),
            "has_more": limit > 0 and total > len(filtered),
        },
    }


class CalcLogger:
    def __init__(self, file_path: str = DEFAULT_CALC_LOG_PATH) -> None:
        self.path = Path(file_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, payload: dict) -> None:
        record = dict(payload)
        record.setdefault("log_version", 2)
        record.setdefault("calc_chain", build_calc_chain(record))
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
