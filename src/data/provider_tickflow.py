from __future__ import annotations

import os
import time as time_module
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
    def _from_tickflow_symbol(symbol: str) -> str:
        value = str(symbol or "").strip().upper()
        if value.endswith(".SH"):
            return f"{value[:-3]}.SS"
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

    @staticmethod
    def _compact_klines_to_dataframe(raw: Any, symbol: str) -> pd.DataFrame:
        if not isinstance(raw, dict) or "timestamp" not in raw:
            return TickFlowProvider._normalize_klines(raw, symbol)

        timestamps = raw.get("timestamp") or []
        if not timestamps:
            return pd.DataFrame()

        trade_time = pd.to_datetime(pd.Series(timestamps), unit="ms", utc=True, errors="coerce")
        trade_time = trade_time.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
        row_count = len(timestamps)

        def _values(name: str, default: float | None = None) -> list:
            values = raw.get(name)
            if values is None:
                return [default] * row_count
            return list(values)

        return pd.DataFrame(
            {
                "symbol": symbol,
                "time": trade_time,
                "open": _values("open"),
                "high": _values("high"),
                "low": _values("low"),
                "close": _values("close"),
                "volume": _values("volume"),
                "amount": _values("amount", 0.0),
            }
        )

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

    def fetch_daily_histories(
        self,
        symbols: list[str],
        start: date,
        end: date,
        adjust: str,
        *,
        batch_size: int = 100,
        request_interval_seconds: float = 2.0,
    ) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
        if end < start:
            start, end = end, start
        normalized_symbols = [str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()]
        if not normalized_symbols:
            return {}, {}

        client = self._get_daily_client()
        if client is None:
            raise RuntimeError("tickflow SDK is not installed, cannot fetch daily history")

        data_by_symbol: dict[str, pd.DataFrame] = {}
        errors: dict[str, str] = {}
        request_start = start - timedelta(days=1)
        batch_limit = max(1, min(int(batch_size or 100), 100))

        chunks = [
            normalized_symbols[index : index + batch_limit]
            for index in range(0, len(normalized_symbols), batch_limit)
        ]
        for chunk_index, chunk in enumerate(chunks):
            tickflow_to_local = {self._to_tickflow_symbol(symbol): symbol for symbol in chunk}
            try:
                raw = client.klines.batch(
                    list(tickflow_to_local.keys()),
                    period="1d",
                    start_time=self._to_milliseconds(request_start),
                    end_time=self._to_milliseconds(end, end_of_day=True),
                    count=10000,
                    adjust=self._adjust_type(adjust),
                    as_dataframe=False,
                    show_progress=False,
                    max_workers=1,
                    batch_size=len(chunk),
                )
                raw_map = raw if isinstance(raw, dict) else {}
                for tickflow_symbol, local_symbol in tickflow_to_local.items():
                    raw_df = raw_map.get(tickflow_symbol)
                    if raw_df is None:
                        raw_df = raw_map.get(local_symbol)
                    data = self._compact_klines_to_dataframe(raw_df, local_symbol)
                    if not data.empty:
                        data["time"] = pd.to_datetime(data["time"], errors="coerce")
                        mask = (data["time"].dt.date >= start) & (data["time"].dt.date <= end)
                        data = data.loc[mask].reset_index(drop=True)
                    data_by_symbol[local_symbol] = data
            except Exception as exc:
                logger.exception("tickflow daily batch fetch failed for %s", ",".join(chunk))
                error_text = str(exc)
                for symbol in chunk:
                    errors[symbol] = error_text

            if request_interval_seconds > 0 and chunk_index < len(chunks) - 1:
                time_module.sleep(float(request_interval_seconds))

        return data_by_symbol, errors

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
