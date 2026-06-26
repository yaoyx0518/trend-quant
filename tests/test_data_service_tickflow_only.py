from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from data.service import DataProviderError, DataService


class FakeTickFlowProvider:
    def __init__(self, daily: pd.DataFrame | None = None, quote: dict | None = None) -> None:
        self.daily = daily if daily is not None else pd.DataFrame()
        self.quote = quote if quote is not None else {"symbol": "518850.SS", "price": None}

    def fetch_daily_history(self, symbol: str, start: date, end: date, adjust: str) -> pd.DataFrame:
        return self.daily.copy()

    def fetch_minute_history(
        self,
        symbol: str,
        period: str,
        count: int,
        adjust: str,
    ) -> pd.DataFrame:
        return self.daily.copy()

    def fetch_latest_quote(self, symbol: str) -> dict:
        return dict(self.quote)

    def fetch_trading_calendar(self, start: date, end: date) -> list[date]:
        return []


class DataServiceTickFlowOnlyTest(unittest.TestCase):
    def test_ignores_fallback_providers(self) -> None:
        service = DataService(provider_priority=["tickflow", "yahoo", "akshare"])

        self.assertEqual(service.provider_priority, ["tickflow"])
        self.assertEqual(list(service.providers), ["tickflow"])

    def test_daily_history_sets_provider(self) -> None:
        service = DataService()
        service.providers["tickflow"] = FakeTickFlowProvider(
            daily=pd.DataFrame(
                [{"time": "2026-06-26", "open": 1, "high": 1, "low": 1, "close": 1}]
            )
        )

        result = service.fetch_daily_history(
            "518850.SS",
            date(2026, 6, 1),
            date(2026, 6, 26),
        )

        self.assertEqual(result.loc[0, "provider"], "tickflow")

    def test_daily_history_empty_raises(self) -> None:
        service = DataService()
        service.providers["tickflow"] = FakeTickFlowProvider()

        with self.assertRaisesRegex(DataProviderError, "TickFlow returned no daily history"):
            service.fetch_daily_history("518850.SS", date(2026, 6, 1), date(2026, 6, 26))

    def test_latest_quote_without_price_raises(self) -> None:
        service = DataService()
        service.providers["tickflow"] = FakeTickFlowProvider(quote={"symbol": "518850.SS"})

        with self.assertRaisesRegex(DataProviderError, "TickFlow returned no latest quote"):
            service.fetch_latest_quote("518850.SS")


if __name__ == "__main__":
    unittest.main()
