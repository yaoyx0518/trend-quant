from __future__ import annotations

import unittest
from datetime import date, timedelta
from unittest.mock import patch

import pandas as pd

from app.routers import market_view
from app.routers.market_view import build_market_payload, compute_market_indicators


def sample_daily_bars(rows: int = 80) -> pd.DataFrame:
    start = date(2026, 1, 1)
    items = []
    price = 1.0
    for idx in range(rows):
        day = start + timedelta(days=idx)
        price += 0.01
        open_price = price
        close_price = price + (0.01 if idx % 2 == 0 else -0.004)
        items.append(
            {
                "time": day.isoformat(),
                "open": open_price,
                "high": max(open_price, close_price) + 0.02,
                "low": min(open_price, close_price) - 0.02,
                "close": close_price,
                "volume": 100000 + idx * 1000,
                "amount": 200000 + idx * 1200,
            }
        )
    return pd.DataFrame(items)


class MarketViewIndicatorTest(unittest.TestCase):
    def test_indicator_series_align_with_daily_bars(self) -> None:
        df = sample_daily_bars()

        indicators = compute_market_indicators(df)

        for group in ("ma", "boll", "macd", "bias", "volume_ma"):
            for series in indicators[group].values():
                self.assertEqual(len(series), len(df), group)

    def test_build_market_payload_uses_one_timeline_for_all_series(self) -> None:
        df = sample_daily_bars()

        payload = build_market_payload("518850.SS", df, "黄金")

        self.assertEqual(payload["meta"]["rows"], len(df))
        self.assertEqual(len(payload["dates"]), len(df))
        self.assertEqual(len(payload["candles"]), len(df))
        self.assertEqual(len(payload["volumes"]), len(df))
        self.assertEqual(payload["display_name"], "黄金")
        for group in payload["indicators"].values():
            for series in group.values():
                self.assertEqual(len(series), len(payload["dates"]))

    def test_moving_average_waits_for_full_window(self) -> None:
        df = sample_daily_bars()

        payload = build_market_payload("518850.SS", df)
        ma20 = payload["indicators"]["ma"]["20"]

        self.assertTrue(all(value is None for value in ma20[:19]))
        self.assertIsNotNone(ma20[19])


class FakeMarketViewDb:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def load_market_data(self, symbol: str) -> pd.DataFrame:
        return self.df.copy()


class MarketViewApiTest(unittest.IsolatedAsyncioTestCase):
    async def test_daily_api_defaults_to_full_local_history(self) -> None:
        df = sample_daily_bars(1300)
        fake_db = FakeMarketViewDb(df)

        with patch.object(market_view, "get_db", return_value=fake_db):
            payload = await market_view.get_market_daily(
                symbol="518850.SS",
                limit=market_view.DEFAULT_LIMIT,
            )

        self.assertEqual(payload["meta"]["rows"], len(df))
        self.assertEqual(payload["meta"]["start"], "2026-01-01")
        self.assertEqual(payload["meta"]["end"], (date(2026, 1, 1) + timedelta(days=1299)).isoformat())


if __name__ == "__main__":
    unittest.main()
