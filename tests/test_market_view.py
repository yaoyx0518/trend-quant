from __future__ import annotations

import unittest
from datetime import date, timedelta
from unittest.mock import patch

import pandas as pd

from app.routers import market_view
from app.routers.market_view import build_market_payload, compute_market_indicators
from strategy.trend_score_core import calculate_trend_score_snapshot


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
        self.assertEqual(len(indicators["rsi"]["series"]), len(df))
        self.assertEqual(indicators["rsi"]["period"], 14)
        self.assertEqual(len(indicators["trend"]["score"]), len(df))
        self.assertEqual(len(indicators["trend"]["ma"]["5"]), len(df))
        self.assertEqual(len(indicators["trend"]["ma"]["10"]), len(df))

    def test_build_market_payload_uses_one_timeline_for_all_series(self) -> None:
        df = sample_daily_bars()

        payload = build_market_payload("518850.SS", df, "Gold")

        self.assertEqual(payload["meta"]["rows"], len(df))
        self.assertEqual(len(payload["dates"]), len(df))
        self.assertEqual(len(payload["candles"]), len(df))
        self.assertEqual(len(payload["volumes"]), len(df))
        self.assertEqual(payload["display_name"], "Gold")
        indicators = payload["indicators"]
        for group_name in ("ma", "boll", "macd", "bias", "volume_ma"):
            for series in indicators[group_name].values():
                self.assertEqual(len(series), len(payload["dates"]), group_name)
        self.assertEqual(len(indicators["rsi"]["series"]), len(payload["dates"]))
        self.assertEqual(len(indicators["trend"]["score"]), len(payload["dates"]))
        self.assertEqual(len(indicators["trend"]["ma"]["5"]), len(payload["dates"]))
        self.assertEqual(len(indicators["trend"]["ma"]["10"]), len(payload["dates"]))

    def test_moving_average_waits_for_full_window(self) -> None:
        df = sample_daily_bars()

        payload = build_market_payload("518850.SS", df)
        ma20 = payload["indicators"]["ma"]["20"]

        self.assertTrue(all(value is None for value in ma20[:19]))
        self.assertIsNotNone(ma20[19])

    def test_trend_indicator_aligns_and_respects_custom_core_periods(self) -> None:
        df = sample_daily_bars(100)

        payload = build_market_payload(
            "518850.SS",
            df,
            trend_cfg={"n_short": 3, "n_mid": 6, "n_long": 12, "atr_period": 8},
        )
        trend = payload["indicators"]["trend"]

        self.assertEqual(payload["meta"]["trend_config"]["n_short"], 3)
        self.assertEqual(payload["meta"]["trend_config"]["n_mid"], 6)
        self.assertEqual(payload["meta"]["trend_config"]["n_long"], 12)
        self.assertEqual(payload["meta"]["trend_config"]["atr_period"], 8)
        self.assertEqual(len(trend["score"]), len(df))
        self.assertTrue(all(value is None for value in trend["score"][:13]))
        self.assertTrue(any(value is not None for value in trend["score"][13:]))

    def test_trend_indicator_matches_snapshot_core_formula(self) -> None:
        df = sample_daily_bars(100)
        cfg = {"n_short": 3, "n_mid": 6, "n_long": 12, "atr_period": 8}

        payload = build_market_payload("518850.SS", df, trend_cfg=cfg)
        snapshot = calculate_trend_score_snapshot(df, cfg)

        self.assertAlmostEqual(
            payload["indicators"]["trend"]["score"][-1],
            round(float(snapshot["trend_score"]), 6),
            places=6,
        )

    def test_build_market_payload_adds_category_label(self) -> None:
        df = sample_daily_bars()

        payload = build_market_payload(
            "510300.SS",
            df,
            "CSI300",
            {
                "category_l1": "Broad",
                "category_l2": "Large Cap",
                "category_l3": "CSI300",
                "factor_tags": ["Value"],
            },
        )

        self.assertEqual(payload["meta"]["category_path"], "Broad-Large Cap-CSI300")
        self.assertIn("Broad-Large Cap-CSI300", payload["display_label"])


class FakeMarketViewDb:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def load_market_data(self, symbol: str) -> pd.DataFrame:
        return self.df.copy()

    def get_instrument_metadata(self, symbol: str) -> dict | None:
        return None


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

    async def test_daily_api_accepts_custom_trend_periods(self) -> None:
        df = sample_daily_bars(120)
        fake_db = FakeMarketViewDb(df)

        with patch.object(market_view, "get_db", return_value=fake_db):
            payload = await market_view.get_market_daily(
                symbol="518850.SS",
                limit=market_view.DEFAULT_LIMIT,
                trend_n_short=4,
                trend_n_mid=9,
                trend_n_long=18,
                trend_atr_period=11,
            )

        self.assertEqual(payload["meta"]["trend_config"]["n_short"], 4)
        self.assertEqual(payload["meta"]["trend_config"]["n_mid"], 9)
        self.assertEqual(payload["meta"]["trend_config"]["n_long"], 18)
        self.assertEqual(payload["meta"]["trend_config"]["atr_period"], 11)

    async def test_daily_api_accepts_custom_rsi_period(self) -> None:
        df = sample_daily_bars(80)
        fake_db = FakeMarketViewDb(df)

        with patch.object(market_view, "get_db", return_value=fake_db):
            payload = await market_view.get_market_daily(
                symbol="518850.SS",
                limit=market_view.DEFAULT_LIMIT,
                rsi_period=7,
            )

        self.assertEqual(payload["meta"]["rsi_config"]["period"], 7)
        self.assertEqual(payload["indicators"]["rsi"]["period"], 7)
        self.assertEqual(len(payload["indicators"]["rsi"]["series"]), len(df))


if __name__ == "__main__":
    unittest.main()
