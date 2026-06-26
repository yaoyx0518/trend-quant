from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

import pandas as pd

from audit.app_logger import get_logger
from data.provider_base import IDataProvider
from data.provider_utils import parse_minute_period, safe_float, standardize_ohlcv

logger = get_logger(__name__)

try:
    from tickflow import TickFlow  # type: ignore
except Exception:  # pragma: no cover
    TickFlow = None


class TickFlowProvider(IDataProvider):
    name = "tickflow"
    free_base_url = "https://free-api.tickflow.org"
    paid_base_url = "https://api.tickflow.org"

    def __init__(self) -> None:
        self.api_key = str(os.getenv("TICKFLOW_API_KEY", "") or "").strip()
        self._daily_client = None
        self._paid_client = None

    @staticmethod
    def _to_tickflow_symbol(symbol: str) -> str:
        value = str(symbol or "").strip().upper()
        if value.endswith(".SS"):
            return f"{value[:-3]}.SH"
        return value

    @staticmethod
    def _to_milliseconds(day: date, *, end_of_day: bool = False) -> int:
        value = time.max if end_of_day else time.min
        dt = datetime.combine(day, value, tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _adjust_type(adjust: str) -> str:
        return {
            "qfq": "forward_additive",
            "hfq": "backward_additive",
            "none": "none",
        }.get(str(adjust or "").strip().lower(), "forward_additive")

    def _get_daily_client(self):
        if TickFlow is None:
            return None
        if self._daily_client is None:
            if self.api_key:
                self._daily_client = TickFlow(
                    api_key=self.api_key,
                    base_url=os.getenv("TICKFLOW_BASE_URL", self.paid_base_url),
                )
            else:
                # Construct directly instead of TickFlow.free() because SDK 0.1.24
                # prints an emoji that can fail under the default Windows GBK console.
                self._daily_client = TickFlow(
                    base_url=os.getenv("TICKFLOW_FREE_BASE_URL", self.free_base_url),
                )
        return self._daily_client

    def _get_paid_client(self):
        if TickFlow is None or not self.api_key:
            return None
        if self._paid_client is None:
            self._paid_client = TickFlow(
                api_key=self.api_key,
                base_url=os.getenv("TICKFLOW_BASE_URL", self.paid_base_url),
            )
        return self._paid_client

    @staticmethod
    def _normalize_klines(raw: Any, symbol: str) -> pd.DataFrame:
        if raw is None:
            return pd.DataFrame()
        data = raw.copy() if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw)
        if data.empty:
            return pd.DataFrame()
        if "time" not in data.columns:
            for candidate in ("trade_time", "trade_date", "timestamp"):
                if candidate in data.columns:
                    data["time"] = data[candidate]
                    break
        return standardize_ohlcv(data, symbol)

    def fetch_daily_history(
        self,
        symbol: str,
        start: date,
        end: date,
        adjust: str,
    ) -> pd.DataFrame:
        if end < start:
            start, end = end, start
        client = self._get_daily_client()
        if client is None:
            raise RuntimeError("tickflow SDK is not installed, cannot fetch daily history")

        try:
            request_start = start - timedelta(days=1)
            raw = client.klines.get(
                self._to_tickflow_symbol(symbol),
                period="1d",
                start_time=self._to_milliseconds(request_start),
                end_time=self._to_milliseconds(end, end_of_day=True),
                count=10000,
                adjust=self._adjust_type(adjust),
                as_dataframe=True,
            )
            data = self._normalize_klines(raw, symbol)
            if data.empty:
                return data
            data["time"] = pd.to_datetime(data["time"], errors="coerce")
            mask = (data["time"].dt.date >= start) & (data["time"].dt.date <= end)
            return data.loc[mask].reset_index(drop=True)
        except Exception as exc:
            logger.exception("tickflow daily fetch failed for %s", symbol)
            raise RuntimeError(f"tickflow daily fetch failed for {symbol}: {exc}") from exc

    def fetch_minute_history(
        self,
        symbol: str,
        period: str,
        count: int,
        adjust: str,
    ) -> pd.DataFrame:
        client = self._get_paid_client()
        if client is None:
            raise RuntimeError("TICKFLOW_API_KEY is required for TickFlow minute history")
        try:
            raw = client.klines.get(
                self._to_tickflow_symbol(symbol),
                period=f"{parse_minute_period(period)}m",
                count=max(int(count), 1),
                adjust=self._adjust_type(adjust),
                as_dataframe=True,
            )
            return self._normalize_klines(raw, symbol)
        except Exception as exc:
            logger.exception("tickflow minute fetch failed for %s", symbol)
            raise RuntimeError(f"tickflow minute fetch failed for {symbol}: {exc}") from exc

    def fetch_latest_quote(self, symbol: str) -> dict:
        client = self._get_paid_client()
        if client is None:
            raise RuntimeError("TICKFLOW_API_KEY is required for TickFlow latest quote")
        try:
            raw = client.quotes.get(symbols=[self._to_tickflow_symbol(symbol)])
            if isinstance(raw, pd.DataFrame):
                item = raw.iloc[0].to_dict() if not raw.empty else {}
            elif isinstance(raw, list):
                item = raw[0] if raw else {}
            elif isinstance(raw, dict):
                item = raw
            else:
                item = {}
            ext = item.get("ext", {}) if isinstance(item.get("ext"), dict) else {}
            return {
                "symbol": symbol,
                "name": str(item.get("name", ext.get("name", "")) or "").strip() or None,
                "price": safe_float(item.get("last_price", item.get("price")), None),
                "open": safe_float(item.get("open"), None),
                "high": safe_float(item.get("high"), None),
                "low": safe_float(item.get("low"), None),
                "volume": safe_float(item.get("volume"), None),
                "amount": safe_float(item.get("amount"), None),
                "ts": str(
                    item.get("trade_time", item.get("timestamp", datetime.now().isoformat()))
                ),
            }
        except Exception as exc:
            logger.exception("tickflow latest quote failed for %s", symbol)
            raise RuntimeError(f"tickflow latest quote failed for {symbol}: {exc}") from exc

    def fetch_instrument_name(self, symbol: str) -> str | None:
        client = self._get_daily_client()
        if client is None:
            raise RuntimeError("tickflow SDK is not installed, cannot fetch instrument name")
        try:
            item = client.instruments.get(self._to_tickflow_symbol(symbol))
            if not isinstance(item, dict):
                return None
            return str(item.get("name", "") or "").strip() or None
        except Exception as exc:
            logger.exception("tickflow instrument fetch failed for %s", symbol)
            raise RuntimeError(f"tickflow instrument fetch failed for {symbol}: {exc}") from exc

    def fetch_trading_calendar(self, start: date, end: date) -> list[date]:
        return []

    def close(self) -> None:
        clients = {
            id(client): client
            for client in (self._daily_client, self._paid_client)
            if client is not None
        }
        for client in clients.values():
            try:
                client.close()
            except Exception as exc:
                logger.debug("tickflow client close failed: %s", exc)
        self._daily_client = None
        self._paid_client = None
