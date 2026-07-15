from __future__ import annotations

import re
import threading
import time
from datetime import date, datetime, timedelta
from typing import Callable

import pandas as pd

from audit.app_logger import get_logger
from core.calendar import is_trading_day as _calendar_is_trading_day
from core.settings import TickFlowSettings, load_settings
from data.provider_tickflow import TickFlowProvider
from data.storage.market_store import MarketStore
from data.storage.runtime_store import RuntimeStore

logger = get_logger(__name__)
_symbol_locks_guard = threading.Lock()
_symbol_locks: dict[str, threading.Lock] = {}


class DataProviderError(RuntimeError):
    """Raised when the configured market data provider cannot return usable data."""


def _symbol_lock(symbol: str) -> threading.Lock:
    key = str(symbol or "").strip().upper()
    with _symbol_locks_guard:
        lock = _symbol_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _symbol_locks[key] = lock
        return lock


def _retry_wait_seconds(errors: dict[str, str], fallback: float) -> float:
    waits: list[float] = []
    for message in errors.values():
        match = re.search(r"请\s*(\d+)\s*ms\s*后重试", str(message))
        if match:
            waits.append(max(0.0, int(match.group(1)) / 1000.0))
    return max([float(fallback), *waits]) if waits else float(fallback)


def _non_retryable_provider_error(errors: dict[str, str]) -> str | None:
    markers = (
        "批量查询权限",
        "无日/周/月K线查询批量查询权限",
        "403 Forbidden",
        "PermissionError",
    )
    for message in errors.values():
        text = str(message or "")
        if any(marker in text for marker in markers):
            return text
    return None


class DataService:
    def __init__(
        self,
        provider_priority: list[str] | None = None,
        tickflow_settings: TickFlowSettings | None = None,
    ) -> None:
        self.tickflow_settings = tickflow_settings or load_settings().tickflow
        self.providers = {
            "tickflow": TickFlowProvider(settings=self.tickflow_settings),
        }
        requested = provider_priority or ["tickflow"]
        ignored = [name for name in requested if name != "tickflow"]
        if ignored:
            logger.warning(
                "Ignoring non-TickFlow data providers %s; TickFlow is the only active provider",
                ignored,
            )
        self.provider_priority = ["tickflow"]
        self.market_store = MarketStore()
        self.runtime_store = RuntimeStore()

    def _ordered_providers(self):
        for name in self.provider_priority:
            provider = self.providers.get(name)
            if provider is not None:
                yield name, provider

    def fetch_daily_history(self, symbol: str, start: date, end: date, adjust: str = "qfq") -> pd.DataFrame:
        name, provider = self._tickflow_provider()
        data = provider.fetch_daily_history(symbol, start, end, adjust)
        if data.empty:
            raise DataProviderError(
                f"TickFlow returned no daily history for {symbol} from {start} to {end}"
            )
        data["provider"] = name
        return data

    def fetch_daily_histories(
        self,
        symbols: list[str],
        start: date,
        end: date,
        adjust: str = "qfq",
        *,
        batch_size: int = 100,
        request_interval_seconds: float = 2.0,
    ) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
        name, provider = self._tickflow_provider()
        fetcher = getattr(provider, "fetch_daily_histories", None)
        if not callable(fetcher):
            data_by_symbol: dict[str, pd.DataFrame] = {}
            errors: dict[str, str] = {}
            for symbol in symbols:
                try:
                    data_by_symbol[symbol] = provider.fetch_daily_history(symbol, start, end, adjust)
                except Exception as exc:
                    errors[symbol] = str(exc)
            for df in data_by_symbol.values():
                if not df.empty:
                    df["provider"] = name
            return data_by_symbol, errors

        data_by_symbol, errors = fetcher(
            symbols,
            start,
            end,
            adjust,
            batch_size=batch_size,
            request_interval_seconds=request_interval_seconds,
        )
        for df in data_by_symbol.values():
            if not df.empty:
                df["provider"] = name
        return data_by_symbol, errors

    def fetch_minute_history(self, symbol: str, period: str = "30", count: int = 48, adjust: str = "qfq") -> pd.DataFrame:
        name, provider = self._tickflow_provider()
        data = provider.fetch_minute_history(symbol, period, count, adjust)
        if data.empty:
            raise DataProviderError(
                f"TickFlow returned no {period} minute history for {symbol}, count={count}"
            )
        data["provider"] = name
        return data

    def fetch_latest_quote(self, symbol: str) -> dict:
        name, provider = self._tickflow_provider()
        quote = provider.fetch_latest_quote(symbol)
        if quote.get("price") is None:
            raise DataProviderError(f"TickFlow returned no latest quote price for {symbol}")
        quote["provider"] = name
        return quote

    def fetch_latest_quotes(self, symbols: list[str]) -> dict[str, dict]:
        """Batch-fetch real-time quotes for multiple symbols."""
        if not symbols:
            return {}
        name, provider = self._tickflow_provider()
        batch_fetcher = getattr(provider, "fetch_latest_quotes", None)
        if not callable(batch_fetcher):
            # Fallback: call single-symbol fetch in a loop.
            result: dict[str, dict] = {}
            for symbol in symbols:
                try:
                    result[symbol] = self.fetch_latest_quote(symbol)
                except Exception as exc:
                    result[symbol] = {"symbol": symbol, "error": str(exc)}
            return result
        quotes = batch_fetcher(symbols)
        for q in quotes.values():
            if "error" not in q:
                q["provider"] = name
        return quotes

    def fetch_instrument_name(self, symbol: str) -> dict:
        name, provider = self._tickflow_provider()
        provider_name_fetcher = getattr(provider, "fetch_instrument_name", None)
        if callable(provider_name_fetcher):
            instrument_name = str(provider_name_fetcher(symbol) or "").strip()
            if instrument_name:
                return {
                    "symbol": symbol,
                    "name": instrument_name,
                    "provider": name,
                    "ts": datetime.now().isoformat(),
                }
        quote = provider.fetch_latest_quote(symbol)
        instrument_name = str(quote.get("name", "") or "").strip()
        if instrument_name:
            return {
                "symbol": symbol,
                "name": instrument_name,
                "provider": name,
                "ts": quote.get("ts"),
            }
        raise DataProviderError(f"TickFlow returned no instrument name for {symbol}")

    def is_trading_day(self, day: date) -> bool:
        # Use the project-level calendar which combines weekday
        # checks with known A-share holiday exclusions.
        return _calendar_is_trading_day(day)

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
        local_start_before, local_end_before = self._date_span(existing)

        fetched = self.fetch_daily_history(symbol, start_date, end_date, adjust=adjust)
        return self._save_backfill_result(
            symbol=symbol,
            requested_start=start_date,
            requested_end=end_date,
            fetched=fetched,
            local_start_before=local_start_before,
            local_end_before=local_end_before,
        )

    def _effective_fetch_start(self, symbol: str, requested_start: date) -> tuple[date, int, str | None, str | None]:
        with _symbol_lock(symbol):
            existing = self.market_store.load_history(symbol)
            existing_rows = int(len(existing))
            local_start_before, local_end_before = self._date_span(existing)
            if existing.empty:
                return requested_start, existing_rows, local_start_before, local_end_before

            existing["time"] = pd.to_datetime(existing["time"], errors="coerce")
            max_time = existing["time"].dropna().max()
            if pd.isna(max_time):
                return requested_start, existing_rows, local_start_before, local_end_before
            return (
                max(requested_start, max_time.date() + timedelta(days=1)),
                existing_rows,
                local_start_before,
                local_end_before,
            )

    def _save_backfill_result(
        self,
        *,
        symbol: str,
        requested_start: date,
        requested_end: date,
        fetched: pd.DataFrame,
        local_start_before: str | None,
        local_end_before: str | None,
    ) -> dict:
        if requested_end < requested_start:
            requested_start, requested_end = requested_end, requested_start

        fetched_rows = int(len(fetched))
        fetched_start, fetched_end = self._date_span(fetched)
        if fetched.empty:
            with _symbol_lock(symbol):
                existing = self.market_store.load_history(symbol)
                existing_rows = int(len(existing))
                local_start_after, local_end_after = self._date_span(existing)
            return {
                "symbol": symbol,
                "status": "no_data",
                "requested_start": requested_start.isoformat(),
                "requested_end": requested_end.isoformat(),
                "rows_before": existing_rows,
                "rows_after": existing_rows,
                "added_rows": 0,
                "fetched_rows": 0,
                "fetched_start": None,
                "fetched_end": None,
                "local_start_before": local_start_before,
                "local_end_before": local_end_before,
                "local_start_after": local_start_after,
                "local_end_after": local_end_after,
                "path": f"sqlite/{symbol}",
            }

        with _symbol_lock(symbol):
            existing = self.market_store.load_history(symbol)
            existing_rows = int(len(existing))
            current_local_start, current_local_end = self._date_span(existing)
            local_start_before = local_start_before or current_local_start
            local_end_before = local_end_before or current_local_end
            to_save = fetched.copy()
            to_save["time"] = pd.to_datetime(to_save["time"], errors="coerce")
            to_save = to_save.dropna(subset=["time"]).drop_duplicates(subset=["time"]).sort_values("time")
            to_save = to_save.reset_index(drop=True)
            path = self.market_store.save_history(symbol, to_save)
            saved = self.market_store.load_history(symbol)
            rows_after = int(len(saved))
            local_start_after, local_end_after = self._date_span(saved)
        return {
            "symbol": symbol,
            "status": "updated",
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
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

    def _up_to_date_result(
        self,
        *,
        symbol: str,
        requested_start: date,
        requested_end: date,
        rows: int,
        local_start: str | None,
        local_end: str | None,
    ) -> dict:
        return {
            "symbol": symbol,
            "status": "up_to_date",
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
            "rows_before": rows,
            "rows_after": rows,
            "added_rows": 0,
            "fetched_rows": 0,
            "fetched_start": None,
            "fetched_end": None,
            "local_start_before": local_start,
            "local_end_before": local_end,
            "local_start_after": local_start,
            "local_end_after": local_end,
            "path": f"sqlite/{symbol}",
        }

    def backfill_daily_histories(
        self,
        items: list[dict],
        end_date: date,
        adjust: str = "qfq",
        *,
        batch_size: int = 100,
        max_retries: int = 3,
        request_interval_seconds: float = 2.0,
        retry_delay_seconds: float = 2.0,
        progress_callback: Callable[[dict], None] | None = None,
    ) -> list[dict]:
        normalized_items: dict[str, date] = {}
        for item in items:
            symbol = str(item.get("symbol") or "").strip().upper()
            if not symbol or symbol in normalized_items:
                continue
            raw_start = item.get("start_date", date(2020, 1, 1))
            start = raw_start if isinstance(raw_start, date) else datetime.strptime(str(raw_start), "%Y-%m-%d").date()
            if end_date < start:
                start = end_date
            normalized_items[symbol] = start

        results: dict[str, dict] = {}
        remaining = list(normalized_items.keys())
        total = len(remaining)

        for attempt in range(max_retries + 1):
            if not remaining:
                break
            if progress_callback:
                progress_callback(
                    {
                        "event": "attempt_start",
                        "attempt": attempt + 1,
                        "max_attempts": max_retries + 1,
                        "remaining": len(remaining),
                        "finished": len(results),
                        "total": total,
                    }
                )

            work: list[dict] = []
            for symbol in remaining:
                requested_start = normalized_items[symbol]
                fetch_start, rows, local_start, local_end = self._effective_fetch_start(symbol, requested_start)
                if fetch_start > end_date:
                    results[symbol] = {
                        "ok": True,
                        "result": self._up_to_date_result(
                            symbol=symbol,
                            requested_start=requested_start,
                            requested_end=end_date,
                            rows=rows,
                            local_start=local_start,
                            local_end=local_end,
                        ),
                    }
                    if progress_callback:
                        progress_callback(
                            {
                                "event": "item_done",
                                "symbol": symbol,
                                "attempt": attempt + 1,
                                "finished": len(results),
                                "total": total,
                            }
                        )
                    continue
                work.append(
                    {
                        "symbol": symbol,
                        "requested_start": requested_start,
                        "fetch_start": fetch_start,
                        "local_start_before": local_start,
                        "local_end_before": local_end,
                    }
                )

            attempt_errors: dict[str, str] = {}
            non_retryable_error: str | None = None
            batch_limit = max(1, min(int(batch_size or 100), 100))
            chunks = [work[index : index + batch_limit] for index in range(0, len(work), batch_limit)]
            for chunk_index, chunk in enumerate(chunks, start=1):
                if not chunk:
                    continue
                symbols = [item["symbol"] for item in chunk]
                fetch_start = min(item["fetch_start"] for item in chunk)
                if progress_callback:
                    progress_callback(
                        {
                            "event": "request_start",
                            "symbols": symbols,
                            "attempt": attempt + 1,
                            "chunk_index": chunk_index,
                            "chunk_total": len(chunks),
                            "finished": len(results),
                            "total": total,
                        }
                    )
                data_by_symbol, errors = self.fetch_daily_histories(
                    symbols,
                    fetch_start,
                    end_date,
                    adjust=adjust,
                    batch_size=batch_limit,
                    request_interval_seconds=0,
                )
                attempt_errors.update(errors)
                chunk_non_retryable_error = _non_retryable_provider_error(errors)
                if chunk_non_retryable_error:
                    non_retryable_error = chunk_non_retryable_error
                    for remaining_item in work:
                        attempt_errors.setdefault(remaining_item["symbol"], non_retryable_error)
                    break
                for item in chunk:
                    symbol = item["symbol"]
                    if symbol in errors:
                        continue
                    fetched = data_by_symbol.get(symbol, pd.DataFrame())
                    if not fetched.empty:
                        fetched = fetched.copy()
                        fetched["time"] = pd.to_datetime(fetched["time"], errors="coerce")
                        mask = (
                            (fetched["time"].dt.date >= item["fetch_start"])
                            & (fetched["time"].dt.date <= end_date)
                        )
                        fetched = fetched.loc[mask].reset_index(drop=True)
                    try:
                        result = self._save_backfill_result(
                            symbol=symbol,
                            requested_start=item["requested_start"],
                            requested_end=end_date,
                            fetched=fetched,
                            local_start_before=item["local_start_before"],
                            local_end_before=item["local_end_before"],
                        )
                    except Exception as exc:
                        attempt_errors[symbol] = str(exc)
                        logger.exception("saving backfilled data failed for %s", symbol)
                        continue
                    results[symbol] = {"ok": True, "result": result}
                    if progress_callback:
                        progress_callback(
                            {
                                "event": "item_done",
                                "symbol": symbol,
                                "attempt": attempt + 1,
                                "finished": len(results),
                                "total": total,
                            }
                        )

                if request_interval_seconds > 0 and chunk_index < len(chunks):
                    time.sleep(float(request_interval_seconds))

            remaining = [symbol for symbol in remaining if symbol not in results]
            if remaining and non_retryable_error:
                for symbol in remaining:
                    results[symbol] = {
                        "ok": False,
                        "symbol": symbol,
                        "error": non_retryable_error,
                    }
                break
            if remaining and attempt < max_retries:
                wait_seconds = _retry_wait_seconds(attempt_errors, retry_delay_seconds)
                if progress_callback:
                    progress_callback(
                        {
                            "event": "retry_sleep",
                            "attempt": attempt + 1,
                            "next_attempt": attempt + 2,
                            "remaining": len(remaining),
                            "wait_seconds": wait_seconds,
                            "finished": len(results),
                            "total": total,
                            "errors": {symbol: attempt_errors.get(symbol, "") for symbol in remaining},
                        }
                    )
                time.sleep(wait_seconds)
            elif remaining:
                for symbol in remaining:
                    results[symbol] = {
                        "ok": False,
                        "symbol": symbol,
                        "error": attempt_errors.get(symbol) or "补齐失败，已达到最大重试次数",
                    }

        return [results[symbol] for symbol in normalized_items.keys() if symbol in results]

    def update_pool_daily(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
        adjust: str = "qfq",
        max_retries: int = 2,
        retry_interval_seconds: float = 5.0,
    ) -> dict:
        results: list[dict] = []
        failed_symbols: list[str] = []

        for symbol in symbols:
            result: dict | None = None
            last_error: str | None = None
            for attempt in range(max_retries + 1):
                try:
                    result = self.ensure_daily_history(symbol, start_date, end_date, adjust=adjust)
                    break
                except Exception as exc:
                    last_error = str(exc)
                    if attempt < max_retries:
                        logger.warning(
                            "update_pool_daily retry %s/%s for %s: %s",
                            attempt + 1,
                            max_retries,
                            symbol,
                            last_error,
                        )
                        time.sleep(float(retry_interval_seconds))
            if result is None:
                result = {"symbol": symbol, "status": "error", "error": last_error}
                failed_symbols.append(symbol)
            results.append(result)

        success_count = sum(1 for r in results if r.get("status") not in ("error", "no_data"))
        failed_count = len(failed_symbols)

        payload = {
            "ts": datetime.now().isoformat(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total": len(symbols),
            "success": success_count,
            "failed": failed_count,
            "failed_symbols": failed_symbols,
            "results": results,
        }
        day = end_date.isoformat()
        self.runtime_store.write_json(f"advice/data_update_{day}.json", payload)

        # Lightweight status file for the web notification bar.
        self.runtime_store.write_json(
            "daily_update_status.json",
            {
                "ts": datetime.now().isoformat(),
                "date": day,
                "total": len(symbols),
                "success": success_count,
                "failed": failed_count,
                "failed_symbols": failed_symbols,
                "completed": True,
            },
        )

        return payload

    def close(self) -> None:
        for provider in self.providers.values():
            close = getattr(provider, "close", None)
            if callable(close):
                close()

    def _tickflow_provider(self):
        provider = self.providers.get("tickflow")
        if provider is None:
            raise DataProviderError("TickFlow provider is not configured")
        return "tickflow", provider
