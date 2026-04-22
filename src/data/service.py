from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from audit.app_logger import get_logger
from data.provider_akshare import AkshareProvider
from data.provider_efinance import EfinanceProvider
from data.storage.market_store import MarketStore
from data.storage.runtime_store import RuntimeStore

logger = get_logger(__name__)


class DataService:
    def __init__(self, provider_priority: list[str] | None = None) -> None:
        self.providers = {
            "efinance": EfinanceProvider(),
            "akshare": AkshareProvider(),
        }
        self.provider_priority = provider_priority or ["efinance", "akshare"]
        self.market_store = MarketStore()
        self.runtime_store = RuntimeStore()

    def _ordered_providers(self):
        for name in self.provider_priority:
            provider = self.providers.get(name)
            if provider is not None:
                yield name, provider

    def fetch_daily_history(self, symbol: str, start: date, end: date, adjust: str = "qfq") -> pd.DataFrame:
        for name, provider in self._ordered_providers():
            data = provider.fetch_daily_history(symbol, start, end, adjust)
            if not data.empty:
                data["provider"] = name
                return data
        return pd.DataFrame()

    def fetch_minute_history(self, symbol: str, period: str = "30", count: int = 48, adjust: str = "qfq") -> pd.DataFrame:
        for name, provider in self._ordered_providers():
            data = provider.fetch_minute_history(symbol, period, count, adjust)
            if not data.empty:
                data["provider"] = name
                return data
        return pd.DataFrame()

    def fetch_latest_quote(self, symbol: str) -> dict:
        for name, provider in self._ordered_providers():
            quote = provider.fetch_latest_quote(symbol)
            if quote.get("price") is not None:
                quote["provider"] = name
                return quote
        return {"symbol": symbol, "price": None, "ts": None, "provider": None}

    def fetch_instrument_name(self, symbol: str) -> dict:
        for name, provider in self._ordered_providers():
            quote = provider.fetch_latest_quote(symbol)
            instrument_name = str(quote.get("name", "") or "").strip()
            if instrument_name:
                return {
                    "symbol": symbol,
                    "name": instrument_name,
                    "provider": name,
                    "ts": quote.get("ts"),
                }
        return {"symbol": symbol, "name": None, "provider": None, "ts": None}

    def is_trading_day(self, day: date) -> bool:
        # Try provider calendars first.
        start = day - timedelta(days=365)
        for _, provider in self._ordered_providers():
            calendar = provider.fetch_trading_calendar(start, day)
            if calendar:
                return day in set(calendar)
        # Conservative fallback when calendar unavailable.
        return day.weekday() < 5

    def ensure_daily_history(self, symbol: str, start_date: date, end_date: date, adjust: str = "qfq") -> dict:
        existing = self.market_store.load_history(symbol)
        if existing.empty:
            fetch_start = start_date
        else:
            existing["time"] = pd.to_datetime(existing["time"], errors="coerce")
            max_time = existing["time"].dropna().max()
            if pd.isna(max_time):
                fetch_start = start_date
            else:
                fetch_start = max(start_date, max_time.date() + timedelta(days=1))

        if fetch_start > end_date:
            return {"symbol": symbol, "status": "up_to_date", "rows": int(len(existing))}

        fetched = self.fetch_daily_history(symbol, fetch_start, end_date, adjust=adjust)
        if fetched.empty:
            return {"symbol": symbol, "status": "no_data", "rows": int(len(existing))}

        merged = pd.concat([existing, fetched], ignore_index=True)
        merged["time"] = pd.to_datetime(merged["time"], errors="coerce")
        merged = merged.dropna(subset=["time"]).drop_duplicates(subset=["time"]).sort_values("time")
        merged = merged.reset_index(drop=True)
        path = self.market_store.save_history(symbol, merged)

        return {
            "symbol": symbol,
            "status": "updated",
            "rows": int(len(merged)),
            "path": str(path),
            "fetched_from": fetch_start.isoformat(),
            "fetched_to": end_date.isoformat(),
        }

    @staticmethod
    def _date_span(df: pd.DataFrame) -> tuple[str | None, str | None]:
        if df.empty or "time" not in df.columns:
            return None, None
        time_series = pd.to_datetime(df["time"], errors="coerce").dropna()
        if time_series.empty:
            return None, None
        return time_series.min().date().isoformat(), time_series.max().date().isoformat()

    def backfill_daily_history(self, symbol: str, start_date: date, end_date: date, adjust: str = "qfq") -> dict:
        if end_date < start_date:
            start_date, end_date = end_date, start_date

        existing = self.market_store.load_history(symbol)
        existing_rows = int(len(existing))
        local_start_before, local_end_before = self._date_span(existing)

        fetched = self.fetch_daily_history(symbol, start_date, end_date, adjust=adjust)
        fetched_rows = int(len(fetched))
        fetched_start, fetched_end = self._date_span(fetched)
        if fetched.empty:
            return {
                "symbol": symbol,
                "status": "no_data",
                "requested_start": start_date.isoformat(),
                "requested_end": end_date.isoformat(),
                "rows_before": existing_rows,
                "rows_after": existing_rows,
                "added_rows": 0,
                "fetched_rows": 0,
                "fetched_start": None,
                "fetched_end": None,
                "local_start_before": local_start_before,
                "local_end_before": local_end_before,
                "local_start_after": local_start_before,
                "local_end_after": local_end_before,
                "path": f"sqlite/{symbol}",
            }

        merged = pd.concat([existing, fetched], ignore_index=True)
        merged["time"] = pd.to_datetime(merged["time"], errors="coerce")
        merged = merged.dropna(subset=["time"]).drop_duplicates(subset=["time"]).sort_values("time")
        merged = merged.reset_index(drop=True)
        path = self.market_store.save_history(symbol, merged)

        rows_after = int(len(merged))
        local_start_after, local_end_after = self._date_span(merged)
        return {
            "symbol": symbol,
            "status": "updated",
            "requested_start": start_date.isoformat(),
            "requested_end": end_date.isoformat(),
            "rows_before": existing_rows,
            "rows_after": rows_after,
            "added_rows": rows_after - existing_rows,
            "fetched_rows": fetched_rows,
            "fetched_start": fetched_start,
            "fetched_end": fetched_end,
            "local_start_before": local_start_before,
            "local_end_before": local_end_before,
            "local_start_after": local_start_after,
            "local_end_after": local_end_after,
            "path": str(path),
        }

    def update_pool_daily(self, symbols: list[str], start_date: date, end_date: date, adjust: str = "qfq") -> dict:
        results = []
        for symbol in symbols:
            results.append(self.ensure_daily_history(symbol, start_date, end_date, adjust=adjust))

        payload = {
            "ts": datetime.now().isoformat(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "results": results,
        }
        day = end_date.isoformat()
        self.runtime_store.write_json(f"advice/data_update_{day}.json", payload)
        return payload
