from __future__ import annotations

import unittest
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from data.storage.db import Database
from rule_backtest import BacktestExecutionConfig, RuleBacktestRequest, SingleSymbolAllInBacktestEngine
from rule_backtest.loader import StrategyLoader
from rule_backtest.service import RuleBacktestService


def make_bars(closes: list[float], highs: list[float] | None = None, lows: list[float] | None = None) -> pd.DataFrame:
    start = date(2026, 1, 1)
    rows = []
    for idx, close in enumerate(closes):
        high = highs[idx] if highs is not None else close + 1
        low = lows[idx] if lows is not None else close - 1
        rows.append(
            {
                "date": start + timedelta(days=idx),
                "open": close,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1000 + idx,
                "amount": close * (1000 + idx),
            }
        )
    return pd.DataFrame(rows)


def literal_entry_strategy(exit_rule: dict) -> dict:
    return {
        "id": "literal_entry",
        "entry": {
            "type": "group",
            "combinator": "all",
            "children": [
                {
                    "left": {"type": "literal", "value": 1},
                    "operator": ">=",
                    "right": {"type": "literal", "value": 0},
                }
            ],
        },
        "exit": {"type": "group", "combinator": "all", "children": [exit_rule]},
    }


class RuleBacktestEngineTest(unittest.TestCase):
    def test_sma20_entry_and_exit_strategy(self) -> None:
        bars = make_bars(([10.0] * 20) + ([12.0] * 5) + ([8.0] * 5))
        strategy = {
            "id": "close_vs_sma20",
            "entry": {
                "type": "group",
                "combinator": "all",
                "children": [
                    {
                        "left": {"type": "price", "field": "close"},
                        "operator": ">=",
                        "right": {
                            "type": "indicator",
                            "name": "sma",
                            "params": {"field": "close", "period": 20},
                        },
                    }
                ],
            },
            "exit": {
                "type": "group",
                "combinator": "all",
                "children": [
                    {
                        "left": {"type": "price", "field": "close"},
                        "operator": "<=",
                        "right": {
                            "type": "indicator",
                            "name": "sma",
                            "params": {"field": "close", "period": 20},
                        },
                    }
                ],
            },
        }

        result = SingleSymbolAllInBacktestEngine().run(
            RuleBacktestRequest(
                strategy=strategy,
                symbol="TEST",
                bars=bars,
                execution=BacktestExecutionConfig(slippage=0.0, fee_rate=0.0, fee_min=0.0),
            )
        )

        sides = [trade["side"] for trade in result["trades"]]
        self.assertEqual(["BUY", "SELL"], sides)
        self.assertEqual("2026-01-20", result["trades"][0]["date"])
        self.assertEqual("2026-01-26", result["trades"][1]["date"])
        self.assertLess(result["summary"]["max_drawdown"], 0)
        self.assertEqual(len(result["debug_log"]), len(bars))
        kline = result["charts"]["kline"]
        self.assertEqual(len(bars), len(kline["dates"]))
        self.assertEqual(len(bars), len(kline["candles"]))
        self.assertIn("20", kline["ma"])
        self.assertEqual(1, len(kline["buy_points"]))
        self.assertEqual(1, len(kline["sell_points"]))

    def test_hard_stop_sells_at_stop_price_with_cost_fields(self) -> None:
        bars = make_bars([100.0, 90.0], highs=[101.0, 91.0], lows=[99.0, 89.0])
        strategy = literal_entry_strategy(
            {
                "left": {"type": "price", "field": "close"},
                "operator": "<=",
                "right": {
                    "type": "state_value",
                    "name": "hard_stop",
                    "params": {"atr_period": 1, "atr_mul": 1.0},
                },
            }
        )

        result = SingleSymbolAllInBacktestEngine().run(
            RuleBacktestRequest(
                strategy=strategy,
                symbol="TEST",
                bars=bars,
                execution=BacktestExecutionConfig(slippage=0.0, fee_rate=0.0, fee_min=0.0),
            )
        )

        sell = result["trades"][1]
        self.assertEqual("hard_stop", sell["reason"])
        self.assertEqual(98.0, sell["reference_price"])
        self.assertEqual(98.0, sell["exec_price"])
        self.assertIn("commission", sell)
        self.assertIn("stamp_tax", sell)
        self.assertEqual(0.0, sell["stamp_tax"])

    def test_chandelier_stop_sells_at_stop_price(self) -> None:
        bars = make_bars(
            [100.0, 109.0, 99.0],
            highs=[101.0, 110.0, 101.0],
            lows=[99.0, 109.0, 100.0],
        )
        strategy = literal_entry_strategy(
            {
                "left": {"type": "price", "field": "close"},
                "operator": "<=",
                "right": {
                    "type": "state_value",
                    "name": "chandelier_stop",
                    "params": {"atr_period": 1, "atr_mul": 1.0},
                },
            }
        )

        result = SingleSymbolAllInBacktestEngine().run(
            RuleBacktestRequest(
                strategy=strategy,
                symbol="TEST",
                bars=bars,
                execution=BacktestExecutionConfig(slippage=0.0, fee_rate=0.0, fee_min=0.0),
            )
        )

        sell = result["trades"][1]
        self.assertEqual("chandelier_stop", sell["reason"])
        self.assertEqual(101.0, sell["reference_price"])
        self.assertEqual(101.0, sell["exec_price"])

    def test_stock_sell_charges_stamp_tax_and_slippage(self) -> None:
        bars = make_bars([100.0, 90.0], highs=[101.0, 91.0], lows=[99.0, 89.0])
        strategy = literal_entry_strategy(
            {
                "left": {"type": "price", "field": "close"},
                "operator": "<=",
                "right": {
                    "type": "state_value",
                    "name": "hard_stop",
                    "params": {"atr_period": 1, "atr_mul": 1.0},
                },
            }
        )

        result = SingleSymbolAllInBacktestEngine().run(
            RuleBacktestRequest(
                strategy=strategy,
                symbol="STOCK",
                bars=bars,
                execution=BacktestExecutionConfig(
                    initial_capital=100000.0,
                    slippage=0.01,
                    fee_rate=0.001,
                    fee_min=5.0,
                    instrument_type="stock",
                    stock_stamp_tax_rate=0.001,
                ),
            )
        )

        buy, sell = result["trades"]
        self.assertAlmostEqual(101.0, buy["exec_price"])
        self.assertAlmostEqual(98.01, sell["exec_price"])
        self.assertGreater(sell["stamp_tax"], 0)
        self.assertGreater(result["summary"]["total_commission"], 0)
        self.assertGreater(result["summary"]["total_stamp_tax"], 0)
        self.assertGreater(result["summary"]["total_trading_cost"], 0)


class RuleBacktestLoaderServiceTest(unittest.TestCase):
    def test_loader_reads_sample_strategy(self) -> None:
        loader = StrategyLoader()
        strategies = loader.list_strategies()
        ids = {item["id"] for item in strategies}
        self.assertIn("close_above_sma20", ids)

        strategy = loader.load("close_above_sma20")
        self.assertEqual("single_symbol_all_in", strategy["trade_mode"])
        self.assertEqual("sma", strategy["entry"]["children"][0]["right"]["name"])

    def test_loader_saves_new_strategy_yaml(self) -> None:
        strategy = {
            "schema_version": 1,
            "id": "test_created_strategy",
            "name": "Test Created Strategy",
            "trade_mode": "single_symbol_all_in",
            "entry": {
                "type": "group",
                "combinator": "all",
                "children": [
                    {
                        "type": "condition",
                        "left": {"type": "price", "field": "close"},
                        "operator": ">=",
                        "right": {
                            "type": "indicator",
                            "name": "sma",
                            "params": {"field": "close", "period": 20},
                        },
                    }
                ],
            },
            "exit": {
                "type": "group",
                "combinator": "all",
                "children": [
                    {
                        "type": "condition",
                        "left": {"type": "price", "field": "close"},
                        "operator": "<=",
                        "right": {
                            "type": "indicator",
                            "name": "sma",
                            "params": {"field": "close", "period": 20},
                        },
                    }
                ],
            },
        }
        with TemporaryDirectory() as tmp:
            loader = StrategyLoader(base_dir=tmp)
            saved = loader.save(strategy)
            loaded = loader.load("test_created_strategy")
            self.assertEqual("test_created_strategy", saved["id"])
            self.assertEqual("Test Created Strategy", loaded["name"])
            self.assertEqual(20, loaded["entry"]["children"][0]["right"]["params"]["period"])

    def test_loader_saves_new_strategy_to_db_when_available(self) -> None:
        strategy = {
            "schema_version": 1,
            "id": "test_created_db_strategy",
            "name": "Test Created DB Strategy",
            "trade_mode": "single_symbol_all_in",
            "entry": {
                "type": "group",
                "combinator": "all",
                "children": [
                    {
                        "type": "condition",
                        "left": {"type": "price", "field": "close"},
                        "operator": ">=",
                        "right": {
                            "type": "indicator",
                            "name": "sma",
                            "params": {"field": "close", "period": 20},
                        },
                    }
                ],
            },
            "exit": {
                "type": "group",
                "combinator": "all",
                "children": [
                    {
                        "type": "condition",
                        "left": {"type": "price", "field": "close"},
                        "operator": "<=",
                        "right": {
                            "type": "indicator",
                            "name": "sma",
                            "params": {"field": "close", "period": 20},
                        },
                    }
                ],
            },
        }
        with TemporaryDirectory() as tmp:
            strategy_dir = Path(tmp) / "strategies"
            loader = StrategyLoader(
                base_dir=strategy_dir,
                db=Database(Path(tmp) / "trend_quant.db"),
            )
            saved = loader.save(strategy)
            loaded = loader.load("test_created_db_strategy")
            listed = {item["id"]: item for item in loader.list_strategies()}

            self.assertEqual("db", saved["storage"])
            self.assertFalse((strategy_dir / "test_created_db_strategy.yaml").exists())
            self.assertEqual("Test Created DB Strategy", loaded["name"])
            self.assertEqual("db", listed["test_created_db_strategy"]["storage"])

    def test_service_generates_strategy_id_when_missing(self) -> None:
        strategy = {
            "schema_version": 1,
            "name": "中文策略名称",
            "trade_mode": "single_symbol_all_in",
            "entry": {
                "type": "group",
                "combinator": "all",
                "children": [
                    {
                        "type": "condition",
                        "left": {"type": "price", "field": "close"},
                        "operator": ">=",
                        "right": {
                            "type": "indicator",
                            "name": "sma",
                            "params": {"field": "close", "period": 20},
                        },
                    }
                ],
            },
            "exit": {
                "type": "group",
                "combinator": "all",
                "children": [
                    {
                        "type": "condition",
                        "left": {"type": "price", "field": "close"},
                        "operator": "<=",
                        "right": {
                            "type": "indicator",
                            "name": "sma",
                            "params": {"field": "close", "period": 20},
                        },
                    }
                ],
            },
        }
        with TemporaryDirectory() as tmp:
            service = RuleBacktestService(
                strategy_loader=StrategyLoader(base_dir=Path(tmp) / "strategies", db=Database(Path(tmp) / "trend_quant.db")),
                market_store=object(),
            )
            saved = service.save_strategy(strategy)

            self.assertTrue(saved["id"].startswith("strategy_"))
            self.assertEqual("db", saved["storage"])
            self.assertEqual("中文策略名称", service.strategy_loader.load(saved["id"])["name"])

    def test_service_runs_with_fake_market_store(self) -> None:
        class FakeMarketStore:
            def load_history(self, symbol: str) -> pd.DataFrame:
                self.symbol = symbol
                return make_bars(([10.0] * 20) + ([12.0] * 5) + ([8.0] * 5))

            def list_stored_symbols(self) -> list[str]:
                return ["TEST"]

        service = RuleBacktestService(strategy_loader=StrategyLoader(), market_store=FakeMarketStore())
        result = service.run(
            {
                "strategy_id": "close_above_sma20",
                "symbol": "TEST",
                "initial_capital": 100000.0,
                "slippage": 0.0,
                "fee_rate": 0.0,
                "fee_min": 0.0,
            }
        )
        self.assertEqual("ok", result["status"])
        self.assertEqual(["BUY", "SELL"], [trade["side"] for trade in result["trades"]])

    def test_service_meta_tolerates_uninitialized_market_store(self) -> None:
        class UninitializedMarketStore:
            def list_stored_symbols(self) -> list[str]:
                raise RuntimeError("Database not initialized")

        service = RuleBacktestService(strategy_loader=StrategyLoader(), market_store=UninitializedMarketStore())
        instruments = service.list_instruments()
        self.assertTrue(isinstance(instruments, list))
        if instruments:
            self.assertFalse(instruments[0]["has_market_data"])

    def test_service_exposes_indicator_metadata(self) -> None:
        service = RuleBacktestService(strategy_loader=StrategyLoader(), market_store=object())
        indicators = service.list_indicators()
        ids = {item["id"] for item in indicators}
        self.assertIn("sma", ids)
        self.assertIn("bias_atr_normed", ids)


if __name__ == "__main__":
    unittest.main()
