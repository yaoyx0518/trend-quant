from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import httpx
import pandas as pd

from audit.app_logger import get_logger
from data.provider_base import IDataProvider
from data.provider_utils import standardize_ohlcv

logger = get_logger(__name__)


class YahooProvider(IDataProvider):
    name = "yahoo"

    @staticmethod
    def _period_ts(day: date) -> int:
        return int(datetime.combine(day, time.min, tzinfo=timezone.utc).timestamp())

    def fetch_daily_history(self, symbol: str, start: date, end: date, adjust: str) -> pd.DataFrame:
        if end < start:
            start, end = end, start

        params = {
            "period1": self._period_ts(start),
            "period2": self._period_ts(end + timedelta(days=1)),
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        }
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}"

        try:
            response = httpx.get(
                url,
                params=params,
                timeout=20.0,
                headers={"User-Agent": "trend-etf-system/0.1"},
            )
            response.raise_for_status()
            payload = response.json()
            result = ((payload.get("chart") or {}).get("result") or [None])[0]
            if not result:
                return pd.DataFrame()

            timestamps = result.get("timestamp") or []
            quote = (((result.get("indicators") or {}).get("quote") or [{}])[0]) or {}

            def at(values: object, idx: int) -> object:
                if not isinstance(values, list) or idx >= len(values):
                    return None
                return values[idx]

            rows = []
            for idx, ts in enumerate(timestamps):
                rows.append(
                    {
                        "time": datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat(),
                        "open": at(quote.get("open"), idx),
                        "high": at(quote.get("high"), idx),
                        "low": at(quote.get("low"), idx),
                        "close": at(quote.get("close"), idx),
                        "volume": at(quote.get("volume"), idx),
                        "amount": None,
                    }
                )
            return standardize_ohlcv(pd.DataFrame(rows), symbol)
        except Exception as exc:
            logger.warning("yahoo daily fetch failed for %s: %s", symbol, exc)
            return pd.DataFrame()

    def fetch_minute_history(self, symbol: str, period: str, count: int, adjust: str) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_latest_quote(self, symbol: str) -> dict:
        return {"symbol": symbol, "price": None, "ts": None}

    def fetch_trading_calendar(self, start: date, end: date) -> list[date]:
        return []
