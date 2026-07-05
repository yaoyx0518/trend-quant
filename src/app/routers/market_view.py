from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.instrument_display import format_symbol_display, strip_etf_suffix
from data.storage.db import get_db

router = APIRouter(prefix="/market-view", tags=["market-view"])
templates = Jinja2Templates(directory="web/templates")

DEFAULT_LIMIT = 20000
MAX_LIMIT = 50000
MA_PERIODS = (5, 10, 20, 30, 60, 120, 200)
BIAS_PERIODS = (6, 12, 24)
VOL_MA_PERIODS = (5, 10)


def _load_yaml(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def _normalize_symbol(raw_symbol: str) -> str:
    text = str(raw_symbol or "").strip().upper()
    if text == "":
        return ""
    if "." in text:
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return text
    suffix = ".SS" if digits.startswith(("5", "6")) else ".SZ"
    return f"{digits}{suffix}"


def _config_name_map() -> dict[str, str]:
    payload = _load_yaml("config/instruments.yaml")
    instruments = payload.get("instruments", []) if isinstance(payload, dict) else []
    if not isinstance(instruments, list):
        return {}

    out: dict[str, str] = {}
    for item in instruments:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        out[symbol] = strip_etf_suffix(str(item.get("name", "") or ""))
    return out


def _num(value: object) -> float | None:
    try:
        n = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if pd.isna(n):
        return None
    return round(n, 6)


def _series(values: Iterable[object]) -> list[float | None]:
    return [_num(v) for v in values]


def _date_only(value: object) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.date().isoformat()


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False, min_periods=span).mean()


def compute_market_indicators(df: pd.DataFrame) -> dict:
    close = pd.to_numeric(df["close"], errors="coerce")
    volume = pd.to_numeric(df.get("volume", pd.Series(index=df.index)), errors="coerce")

    ma = {
        str(period): _series(close.rolling(period, min_periods=period).mean())
        for period in MA_PERIODS
    }

    boll_mid = close.rolling(20, min_periods=20).mean()
    boll_std = close.rolling(20, min_periods=20).std(ddof=0)
    boll = {
        "mid": _series(boll_mid),
        "upper": _series(boll_mid + 2 * boll_std),
        "lower": _series(boll_mid - 2 * boll_std),
    }

    ema_short = _ema(close, 12)
    ema_long = _ema(close, 26)
    dif = ema_short - ema_long
    dea = _ema(dif, 9)
    macd_bar = (dif - dea) * 2
    macd = {
        "dif": _series(dif),
        "dea": _series(dea),
        "bar": _series(macd_bar),
    }

    bias: dict[str, list[float | None]] = {}
    for period in BIAS_PERIODS:
        ma_n = close.rolling(period, min_periods=period).mean()
        bias[str(period)] = _series((close - ma_n) / ma_n * 100)

    volume_ma = {
        str(period): _series(volume.rolling(period, min_periods=period).mean())
        for period in VOL_MA_PERIODS
    }

    return {
        "ma": ma,
        "boll": boll,
        "macd": macd,
        "bias": bias,
        "volume_ma": volume_ma,
    }


def build_market_payload(symbol: str, df: pd.DataFrame, name: str = "") -> dict:
    if df.empty:
        return {
            "symbol": symbol,
            "name": name,
            "display_name": format_symbol_display(symbol, name),
            "dates": [],
            "candles": [],
            "volumes": [],
            "amounts": [],
            "indicators": {},
            "meta": {"rows": 0, "start": None, "end": None},
        }

    data = df.copy()
    data["time"] = pd.to_datetime(data["time"], errors="coerce")
    data = data.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    for col in ("open", "high", "low", "close", "volume", "amount"):
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    dates = [_date_only(v) for v in data["time"]]
    candles = [
        [_num(row.open), _num(row.close), _num(row.low), _num(row.high)]
        for row in data.itertuples(index=False)
    ]
    volumes = _series(data.get("volume", pd.Series(index=data.index)))
    amounts = _series(data.get("amount", pd.Series(index=data.index)))
    indicators = compute_market_indicators(data)

    return {
        "symbol": symbol,
        "name": name,
        "display_name": format_symbol_display(symbol, name),
        "dates": dates,
        "candles": candles,
        "volumes": volumes,
        "amounts": amounts,
        "indicators": indicators,
        "meta": {
            "rows": int(len(data)),
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
            "ma_periods": list(MA_PERIODS),
            "bias_periods": list(BIAS_PERIODS),
            "volume_ma_periods": list(VOL_MA_PERIODS),
        },
    }


@router.get("", response_class=HTMLResponse)
async def market_view_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        name="market_view.html",
        request=request,
        context={"title": "标的查看"},
    )


@router.get("/api/symbols")
async def list_market_symbols() -> dict:
    name_map = _config_name_map()
    symbols = get_db().list_market_symbols()
    items = [
        {
            "symbol": symbol,
            "name": name_map.get(symbol, ""),
            "display_name": format_symbol_display(symbol, name_map.get(symbol, "")),
        }
        for symbol in symbols
    ]
    return {"items": items, "count": len(items)}


@router.get("/api/daily")
async def get_market_daily(
    symbol: str = Query(..., min_length=1),
    start_date: str = "",
    end_date: str = "",
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
) -> dict:
    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        raise HTTPException(status_code=400, detail="标的无效")

    df = get_db().load_market_data(normalized_symbol)
    if df.empty:
        raise HTTPException(status_code=404, detail="未找到本地日 K 数据")

    data = df.copy()
    data["time"] = pd.to_datetime(data["time"], errors="coerce")
    data = data.dropna(subset=["time"]).sort_values("time")

    if end_date.strip():
        end_ts = pd.to_datetime(end_date, errors="coerce")
    else:
        end_ts = data["time"].max()
    if pd.isna(end_ts):
        raise HTTPException(status_code=404, detail="日 K 日期无效")

    if start_date.strip():
        start_ts = pd.to_datetime(start_date, errors="coerce")
        if pd.isna(start_ts):
            raise HTTPException(status_code=400, detail="开始日期格式应为 YYYY-MM-DD")
    else:
        start_ts = data["time"].min()

    if start_ts > end_ts:
        raise HTTPException(status_code=400, detail="开始日期不能晚于结束日期")

    data = data[(data["time"] >= start_ts) & (data["time"] <= end_ts)]
    if len(data) > limit:
        data = data.tail(limit)

    name = _config_name_map().get(normalized_symbol, "")
    payload = build_market_payload(normalized_symbol, data, name)
    payload["meta"]["requested_start"] = _date_only(start_ts)
    payload["meta"]["requested_end"] = _date_only(end_ts)
    payload["meta"]["limit"] = int(limit)
    return payload
