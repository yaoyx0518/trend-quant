"""Integration tests for engine.signal_engine — SignalEngine with isolated DB.

Uses a mocked DataService so no real TickFlow calls are made.
"""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from engine.signal_engine import SignalEngine


def _make_bars_df(n: int = 50, base_price: float = 10.0) -> pd.DataFrame:
    """Generate clean OHLCV bars for mocking market data."""
    import numpy as np
    rng = np.random.default_rng(42)
    records = []
    price = base_price
    for i in range(n):
        change = 0.01 + rng.normal(0, 0.005)
        close = price * (1 + change)
        records.append({
            "time": f"2025-01-{i+1:02d}",
            "open": price, "high": close * 1.01, "low": close * 0.99,
            "close": close, "volume": 1_000_000 + i * 10_000,
        })
        price = close
    return pd.DataFrame(records)


class TestSignalEngine:
    @pytest.fixture
    def mock_data_service(self) -> MagicMock:
        ds = MagicMock()
        ds.is_trading_day.return_value = True
        ds.fetch_daily_history.return_value = _make_bars_df(60)
        ds.fetch_latest_quote.return_value = {
            "symbol": "A.SS", "price": 12.0, "volume": 500_000,
            "open": 11.5, "high": 12.2, "low": 11.4,
            "time": "2025-01-15 10:00:00",
        }
        ds.fetch_instrument_name.return_value = {"name": "Test ETF"}
        ds.update_pool_daily.return_value = {"status": "ok"}
        return ds

    def test_is_trading_day_delegates(self, mock_data_service, test_db) -> None:
        from unittest.mock import patch

        with patch("engine.signal_engine.DataService", return_value=mock_data_service):
            engine = SignalEngine()
            assert engine.is_trading_day(date(2025, 1, 6)) is True

    def test_run_poll_produces_signals(self, mock_data_service, test_db) -> None:
        """End-to-end: run_poll with mocked market data + isolated DB."""
        # Setup: instruments in test DB
        test_db.save_instrument_metadata([
            {"symbol": "A.SS", "name": "Test A", "category_l1": "宽基",
             "category_l2": "大盘", "category_l3": "沪深300",
             "priority_l1": 1, "priority_l2": 1, "priority_l3": 1, "sort_order": 1},
        ])

        from unittest.mock import patch

        with patch("engine.signal_engine.DataService", return_value=mock_data_service):
            with patch("engine.signal_engine.RuntimeStore") as mock_rs:
                engine = SignalEngine(initial_capital=200_000)
                result = engine.run_poll("poll_30m")

        assert "signals" in result
        assert isinstance(result["signals"], list)

    def test_run_daily_update(self, mock_data_service, test_db) -> None:
        test_db.save_instrument_metadata([
            {"symbol": "A.SS", "name": "Test A", "category_l1": "宽基",
             "category_l2": "大盘", "category_l3": "沪深300",
             "priority_l1": 1, "priority_l2": 1, "priority_l3": 1, "sort_order": 1},
        ])

        from unittest.mock import patch

        with patch("engine.signal_engine.DataService", return_value=mock_data_service):
            with patch("engine.signal_engine.RuntimeStore") as mock_rs:
                engine = SignalEngine()
                result = engine.run_daily_update()

        assert "status" in result
