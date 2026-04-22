from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd

from audit.app_logger import get_logger
from data.provider_base import IDataProvider
from data.provider_utils import normalize_symbol, parse_minute_period, safe_float, standardize_ohlcv

logger = get_logger(__name__)

try:
    import efinance as ef  # type: ignore
except Exception:  # pragma: no cover
    ef = None


class EfinanceProvider(IDataProvider):
    name = "efinance"

    def fetch_daily_history(self, symbol: str, start: date, end: date, adjust: str) -> pd.DataFrame:
        if ef is None:
            logger.warning("efinance not installed, skip daily fetch")
            return pd.DataFrame()

        code = normalize_symbol(symbol)
        beg = start.strftime("%Y%m%d")
        finish = end.strftime("%Y%m%d")

        fqt_map = {"none": 0, "qfq": 1, "hfq": 2}
        fqt = fqt_map.get(adjust.lower(), 1)

        try:
            raw = ef.stock.get_quote_history(stock_codes=code, beg=beg, end=finish, klt=101, fqt=fqt)
            if isinstance(raw, list):
                raw = raw[0] if raw else pd.DataFrame()
            return standardize_ohlcv(raw, symbol)
        except Exception as exc:
            logger.warning("efinance daily fetch failed for %s: %s", symbol, exc)
            return pd.DataFrame()

    def fetch_minute_history(self, symbol: str, period: str, count: int, adjust: str) -> pd.DataFrame:
        if ef is None:
            return pd.DataFrame()

        code = normalize_symbol(symbol)
        klt = int(parse_minute_period(period))
        finish = date.today()
        start = finish - timedelta(days=60)
        fqt_map = {"none": 0, "qfq": 1, "hfq": 2}
        fqt = fqt_map.get(adjust.lower(), 1)

        try:
            raw = ef.stock.get_quote_history(
                stock_codes=code,
                beg=start.strftime("%Y%m%d"),
                end=finish.strftime("%Y%m%d"),
                klt=klt,
                fqt=fqt,
            )
            if isinstance(raw, list):
                raw = raw[0] if raw else pd.DataFrame()
            normalized = standardize_ohlcv(raw, symbol)
            if count > 0 and not normalized.empty:
                normalized = normalized.tail(count).reset_index(drop=True)
            return normalized
        except Exception as exc:
            logger.warning("efinance minute fetch failed for %s: %s", symbol, exc)
            return pd.DataFrame()

    def fetch_latest_quote(self, symbol: str) -> dict:
        if ef is None:
            return {"symbol": symbol, "price": None, "ts": None}

        code = normalize_symbol(symbol)
        try:
            df = ef.stock.get_latest_quote([code])
            if df is None or df.empty:
                return {"symbol": symbol, "price": None, "ts": None}

            row = df.iloc[0]
            ts = row.get("更新时间", row.get("时间", datetime.now().isoformat()))

            return {
                "symbol": symbol,
                "name": str(row.get("名称", row.get("股票名称", "")) or "").strip() or None,
                "price": safe_float(row.get("最新价", row.get("收盘", row.get("close", None))), None),
                "open": safe_float(row.get("今开", row.get("开盘", None)), None),
                "high": safe_float(row.get("最高", None), None),
                "low": safe_float(row.get("最低", None), None),
                "volume": safe_float(row.get("成交量", row.get("量比", None)), None),
                "amount": safe_float(row.get("成交额", None), None),
                "ts": str(ts),
            }
        except Exception as exc:
            logger.warning("efinance latest quote failed for %s: %s", symbol, exc)
            return {"symbol": symbol, "price": None, "ts": None}

    def fetch_trading_calendar(self, start: date, end: date) -> list[date]:
        # efinance does not expose stable trade-calendar API; fallback to weekdays in V1.
        cursor = start
        out: list[date] = []
        while cursor <= end:
            if cursor.weekday() < 5:
                out.append(cursor)
            cursor += timedelta(days=1)
        return out
