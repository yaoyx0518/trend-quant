from __future__ import annotations

import os
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from data.provider_tickflow import TickFlowProvider


class TickFlowProviderTest(unittest.TestCase):
    def test_symbol_and_adjust_mapping(self) -> None:
        self.assertEqual(TickFlowProvider._to_tickflow_symbol("518850.SS"), "518850.SH")
        self.assertEqual(TickFlowProvider._to_tickflow_symbol("159915.SZ"), "159915.SZ")
        self.assertEqual(TickFlowProvider._adjust_type("qfq"), "forward_additive")
        self.assertEqual(TickFlowProvider._adjust_type("hfq"), "backward_additive")
        self.assertEqual(TickFlowProvider._adjust_type("none"), "none")

    @patch.dict(os.environ, {}, clear=True)
    @patch("data.provider_tickflow.TickFlow")
    def test_daily_history_uses_free_service_and_normalizes_schema(
        self,
        tickflow_cls: MagicMock,
    ) -> None:
        client = tickflow_cls.return_value
        client.klines.get.return_value = pd.DataFrame(
            [
                {
                    "symbol": "518850.SH",
                    "trade_date": "2026-06-25",
                    "open": 8.433,
                    "high": 8.433,
                    "low": 8.300,
                    "close": 8.364,
                    "volume": 842693,
                    "amount": 706185676.0,
                }
            ]
        )

        provider = TickFlowProvider()
        result = provider.fetch_daily_history(
            "518850.SS",
            date(2026, 6, 1),
            date(2026, 6, 25),
            "qfq",
        )

        tickflow_cls.assert_called_once_with(base_url="https://free-api.tickflow.org")
        _, kwargs = client.klines.get.call_args
        self.assertEqual(kwargs["period"], "1d")
        self.assertEqual(kwargs["adjust"], "forward_additive")
        self.assertEqual(kwargs["start_time"], TickFlowProvider._to_milliseconds(date(2026, 5, 31)))
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["symbol"], "518850.SS")
        self.assertEqual(result.iloc[0]["amount"], 706185676.0)
        provider.close()
        client.close.assert_called_once()

    @patch.dict(os.environ, {}, clear=True)
    @patch("data.provider_tickflow.TickFlow")
    def test_daily_histories_use_batch_endpoint_and_map_symbols(
        self,
        tickflow_cls: MagicMock,
    ) -> None:
        client = tickflow_cls.return_value
        client.klines.batch.return_value = {
            "518850.SH": pd.DataFrame(
                [
                    {
                        "symbol": "518850.SH",
                        "trade_date": "2026-06-25",
                        "open": 8.433,
                        "high": 8.433,
                        "low": 8.300,
                        "close": 8.364,
                        "volume": 842693,
                        "amount": 706185676.0,
                    }
                ]
            ),
            "159915.SZ": pd.DataFrame(
                [
                    {
                        "symbol": "159915.SZ",
                        "trade_date": "2026-06-25",
                        "open": 2,
                        "high": 2,
                        "low": 2,
                        "close": 2,
                        "volume": 100,
                        "amount": 200,
                    }
                ]
            ),
        }

        provider = TickFlowProvider()
        data, errors = provider.fetch_daily_histories(
            ["518850.SS", "159915.SZ"],
            date(2026, 6, 1),
            date(2026, 6, 25),
            "qfq",
            batch_size=100,
            request_interval_seconds=0,
        )

        self.assertEqual(errors, {})
        _, kwargs = client.klines.batch.call_args
        self.assertEqual(client.klines.batch.call_args.args[0], ["518850.SH", "159915.SZ"])
        self.assertFalse(kwargs["as_dataframe"])
        self.assertEqual(kwargs["max_workers"], 1)
        self.assertEqual(kwargs["batch_size"], 2)
        self.assertIn("518850.SS", data)
        self.assertEqual(data["518850.SS"].iloc[0]["symbol"], "518850.SS")
        self.assertEqual(data["159915.SZ"].iloc[0]["amount"], 200)

    @patch.dict(os.environ, {}, clear=True)
    def test_free_service_does_not_claim_realtime_support(self) -> None:
        provider = TickFlowProvider()
        with self.assertRaisesRegex(RuntimeError, "TICKFLOW_API_KEY is required"):
            provider.fetch_latest_quote("518850.SS")
        with self.assertRaisesRegex(RuntimeError, "TICKFLOW_API_KEY is required"):
            provider.fetch_minute_history("518850.SS", "30", 10, "qfq")


if __name__ == "__main__":
    unittest.main()
