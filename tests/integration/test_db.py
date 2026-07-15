"""Integration tests for data.storage.db — Database CRUD operations.

Uses an isolated SQLite database via the autouse ``isolate_get_db`` fixture
from ``tests/integration/conftest.py``.
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest


class TestMarketDataCRUD:
    def test_save_and_load(self, test_db) -> None:
        df = pd.DataFrame([{
            "time": "2025-01-06",
            "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
            "volume": 1_000_000, "amount": 10_500_000, "provider": "test",
        }])
        test_db.save_market_data("TEST.SS", df, price_mode="qfq")
        loaded = test_db.load_market_data("TEST.SS", price_mode="qfq")
        assert len(loaded) == 1
        assert float(loaded.iloc[0]["close"]) == 10.5

    def test_separate_price_modes(self, test_db) -> None:
        qfq_df = pd.DataFrame([{
            "time": "2025-01-06", "open": 10.0, "high": 11.0,
            "low": 9.0, "close": 10.5, "volume": 1_000_000, "amount": 1_050_000,
            "provider": "test",
        }])
        raw_df = pd.DataFrame([{
            "time": "2025-01-06", "open": 12.0, "high": 13.0,
            "low": 11.0, "close": 12.5, "volume": 1_000_000, "amount": 1_250_000,
            "provider": "test",
        }])
        test_db.save_market_data("TEST2.SS", qfq_df, price_mode="qfq")
        test_db.save_market_data("TEST2.SS", raw_df, price_mode="raw")
        assert float(test_db.load_market_data("TEST2.SS", "qfq").iloc[0]["close"]) == 10.5
        assert float(test_db.load_market_data("TEST2.SS", "raw").iloc[0]["close"]) == 12.5

    def test_list_market_symbols(self, test_db) -> None:
        df = pd.DataFrame([{
            "time": "2025-01-06", "open": 10.0, "high": 11.0,
            "low": 9.0, "close": 10.5, "volume": 1_000_000, "amount": 1_050_000,
            "provider": "test",
        }])
        test_db.save_market_data("A.SS", df, price_mode="qfq")
        test_db.save_market_data("B.SZ", df, price_mode="qfq")
        symbols = test_db.list_market_symbols(price_mode="qfq")
        assert "A.SS" in symbols
        assert "B.SZ" in symbols

    def test_get_market_data_summary(self, test_db) -> None:
        df = pd.DataFrame([{
            "time": "2025-01-06", "open": 10.0, "high": 11.0,
            "low": 9.0, "close": 10.5, "volume": 1_000_000, "amount": 1_050_000,
            "provider": "test",
        }])
        test_db.save_market_data("SUM.SS", df, price_mode="qfq")
        summary = test_db.get_market_data_summary("SUM.SS", "qfq")
        assert summary["rows"] == 1
        assert summary["symbol"] == "SUM.SS" if "symbol" in summary else True


class TestSignalsAndState:
    def test_save_and_get_signals(self, test_db) -> None:
        payload = {
            "signals": [{"symbol": "A.SS", "trend_score": 25.0}],
            "portfolio_snapshot": {},
        }
        test_db.save_signals("2025-01-06", payload)
        loaded = test_db.get_signals("2025-01-06")
        assert loaded is not None
        assert loaded["signals"][0]["symbol"] == "A.SS"

    def test_list_signal_days(self, test_db) -> None:
        test_db.save_signals("2025-01-06", {"signals": [], "portfolio_snapshot": {}})
        test_db.save_signals("2025-01-07", {"signals": [], "portfolio_snapshot": {}})
        days = test_db.list_signal_days()
        assert "2025-01-06" in days
        assert "2025-01-07" in days

    def test_save_and_get_signal_state(self, test_db) -> None:
        states = {"A.SS": {"trend_score": 30.0, "prev_trend_score": 25.0}}
        test_db.save_signal_state(states)
        state = test_db.get_signal_state("A.SS")
        assert state is not None
        assert state["trend_score"] == 30.0

    def test_get_all_signal_states(self, test_db) -> None:
        states = {"A.SS": {"trend_score": 10.0}, "B.SZ": {"trend_score": 20.0}}
        test_db.save_signal_state(states)
        all_states = test_db.get_all_signal_states()
        assert all_states["A.SS"]["trend_score"] == 10.0
        assert all_states["B.SZ"]["trend_score"] == 20.0

    def test_try_acquire_signal_run(self, test_db) -> None:
        assert test_db.try_acquire_signal_run("run_001", "poll_30m", "sig") is True
        # Duplicate should fail
        assert test_db.try_acquire_signal_run("run_001", "poll_30m", "sig") is False

    def test_position_snapshot(self, test_db) -> None:
        snapshot = {"positions": [{"symbol": "A.SS", "qty": 100}]}
        test_db.save_position_snapshot(snapshot)
        loaded = test_db.get_latest_position_snapshot()
        assert loaded is not None
        assert loaded["positions"][0]["symbol"] == "A.SS"


class TestTrades:
    def test_add_and_get_trades(self, test_db) -> None:
        trade = {"symbol": "A.SS", "side": "BUY", "qty": 100, "price": 10.0,
                 "fee": 1.0, "pnl": 0.0, "trade_date": "2025-01-06",
                 "trade_time": "10:00:00", "note": ""}
        trade_id = test_db.add_trade(trade)
        assert trade_id > 0
        trades = test_db.get_trades_by_date("2025-01-06")
        assert len(trades) == 1
        assert float(trades[0]["price"]) == 10.0

    def test_get_all_trades(self, test_db) -> None:
        test_db.add_trade({"symbol": "A", "side": "BUY", "qty": 100, "price": 10.0,
                           "fee": 1.0, "pnl": 0.0, "trade_date": "2025-01-06",
                           "trade_time": "10:00:00", "note": ""})
        test_db.add_trade({"symbol": "B", "side": "SELL", "qty": 50, "price": 12.0,
                           "fee": 1.0, "pnl": 100.0, "trade_date": "2025-01-07",
                           "trade_time": "14:00:00", "note": ""})
        all_trades = test_db.get_all_trades()
        assert len(all_trades) >= 2


class TestInstrumentMetadata:
    def test_save_and_list(self, test_db) -> None:
        test_db.save_instrument_metadata([
            {"symbol": "A.SS", "name": "Alpha", "category_l1": "宽基",
             "category_l2": "大盘", "category_l3": "沪深300", "sort_order": 1},
        ])
        items = test_db.list_instrument_metadata()
        assert len(items) == 1
        assert items[0]["symbol"] == "A.SS"

    def test_get_instrument_metadata_map(self, test_db) -> None:
        test_db.save_instrument_metadata([
            {"symbol": "A.SS", "name": "Alpha"},
            {"symbol": "B.SZ", "name": "Beta"},
        ])
        m = test_db.get_instrument_metadata_map()
        assert m["A.SS"]["name"] == "Alpha"
        assert m["B.SZ"]["name"] == "Beta"

    def test_instrument_categories(self, test_db) -> None:
        test_db.save_instrument_categories([
            {"path": "宽基", "level": 1, "name": "宽基", "priority": 1},
            {"path": "宽基-大盘", "level": 2, "name": "大盘", "parent_path": "宽基", "priority": 1},
        ])
        cats = test_db.list_instrument_categories()
        assert len(cats) == 2

    def test_replace_instrument_categories(self, test_db) -> None:
        test_db.save_instrument_categories([{"path": "old", "level": 1, "name": "Old", "priority": 1}])
        n = test_db.replace_instrument_categories([{"path": "new", "level": 1, "name": "New", "priority": 1}])
        assert n == 1
        cats = test_db.list_instrument_categories()
        assert cats[0]["path"] == "new"


class TestRuleStrategies:
    def test_save_and_list(self, test_db) -> None:
        strategy = {
            "schema_version": 1,
            "id": "test_rule",
            "name": "Test Rule",
            "trade_mode": "single_symbol_all_in",
        }
        test_db.save_rule_strategy(strategy)
        items = test_db.list_rule_strategies()
        assert any(s["id"] == "test_rule" for s in items)

    def test_get_and_delete(self, test_db) -> None:
        test_db.save_rule_strategy({"schema_version": 1, "id": "del_me", "name": "X", "trade_mode": "single_symbol_all_in"})
        assert test_db.get_rule_strategy("del_me") is not None
        assert test_db.delete_rule_strategy("del_me") is True
        assert test_db.get_rule_strategy("del_me") is None


class TestBacktests:
    def test_save_and_list_backtests(self, test_db) -> None:
        result = {"strategy_id": "trend_score_v1", "metrics": {"total_return": 0.1}}
        test_db.save_backtest("bt_001", result)
        items = test_db.list_backtests(limit=10)
        assert len(items) >= 1

    def test_favorite_toggle(self, test_db) -> None:
        test_db.save_backtest("bt_fav", {"strategy_id": "x"})
        assert test_db.set_backtest_favorite("bt_fav", True) is True
        items = test_db.list_backtests()
        fav = next(i for i in items if i["run_id"] == "bt_fav")
        assert fav["is_favorite"] is True

    def test_delete_backtest(self, test_db) -> None:
        test_db.save_backtest("bt_del", {"strategy_id": "x"})
        assert test_db.delete_backtest("bt_del") is True
        assert test_db.get_backtest("bt_del") is None


class TestOptimizations:
    def test_save_and_get_optimization(self, test_db) -> None:
        test_db.save_optimization_job("opt_001", {"status": "running"}, None)
        status = test_db.get_optimization_status("opt_001")
        assert status["status"] == "running"

    def test_save_with_result(self, test_db) -> None:
        test_db.save_optimization_job("opt_002", {"status": "done"}, {"best_score": 1.5})
        result = test_db.get_optimization_result("opt_002")
        assert result is not None
