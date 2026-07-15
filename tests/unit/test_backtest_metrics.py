"""Unit tests for backtest.metrics — performance metric computations."""

from __future__ import annotations

import pytest

from backtest.metrics import (
    compute_drawdown,
    compute_metrics,
    compute_annual_returns,
    compute_monthly_heatmap,
    compute_symbol_trade_stats,
)


def _make_nav(equities: list[float]) -> list[dict]:
    """helper: create daily_nav list from equity values."""
    return [{"date": f"2025-01-{i+1:02d}", "equity": e} for i, e in enumerate(equities)]


class TestComputeDrawdown:
    def test_all_time_high(self) -> None:
        """No drawdown when equity monotonically rises."""
        nav = _make_nav([100_000, 101_000, 102_000, 103_000])
        dd = compute_drawdown(nav)
        assert dd == [0.0, 0.0, 0.0, 0.0]

    def test_with_drawdown(self) -> None:
        """Drawdown when equity drops from peak."""
        nav = _make_nav([100_000, 90_000, 95_000])
        dd = compute_drawdown(nav)
        assert dd[1] == pytest.approx(-0.1, abs=0.01)
        assert dd[2] == pytest.approx(-0.05, abs=0.01)

    def test_empty_nav(self) -> None:
        assert compute_drawdown([]) == []


class TestComputeMetrics:
    def test_returns_dict_with_keys(self) -> None:
        nav = _make_nav([200_000, 210_000, 205_000, 215_000])
        trades: list[dict] = []
        result = compute_metrics(nav, trades, 0.0)
        for key in ("total_return", "annual_return", "max_drawdown", "sharpe",
                     "win_rate", "calmar", "sortino", "turnover"):
            assert key in result, f"Missing key: {key}"

    def test_win_rate_calculation(self) -> None:
        nav = _make_nav([200_000, 210_000])
        trades = [
            {"side": "SELL", "pnl": 500},
            {"side": "SELL", "pnl": -200},
            {"side": "SELL", "pnl": 300},
        ]
        result = compute_metrics(nav, trades, 0.0)
        assert result["win_rate"] == pytest.approx(2 / 3, abs=0.01)
        assert result["trade_count"] == 3

    def test_zero_trades(self) -> None:
        nav = _make_nav([200_000, 210_000])
        result = compute_metrics(nav, [], 0.0)
        assert result["win_rate"] == 0.0
        assert result["trade_count"] == 0


class TestComputeAnnualReturns:
    def test_single_year(self) -> None:
        nav = [{"date": "2025-01-02", "equity": 200_000},
               {"date": "2025-12-31", "equity": 220_000}]
        result = compute_annual_returns(nav)
        assert len(result) >= 1
        assert result[0]["year"] == 2025

    def test_empty(self) -> None:
        assert compute_annual_returns([]) == []


class TestComputeMonthlyHeatmap:
    def test_structure(self) -> None:
        nav = _make_nav([200_000, 210_000, 205_000] * 5)
        result = compute_monthly_heatmap(nav)
        assert "years" in result
        assert "months" in result
        assert "data" in result
        assert isinstance(result["data"], list)


class TestComputeSymbolTradeStats:
    def test_per_symbol_aggregation(self) -> None:
        trades = [
            {"symbol": "A", "side": "SELL", "pnl": 100},
            {"symbol": "A", "side": "SELL", "pnl": -50},
            {"symbol": "B", "side": "SELL", "pnl": 200},
        ]
        stats = compute_symbol_trade_stats(trades)
        symbols = {s["symbol"] for s in stats}
        assert symbols == {"A", "B"}
        # Verify win rate for symbol A (1 win / 2 trades = 0.5)
        a_stat = next(s for s in stats if s["symbol"] == "A")
        assert a_stat["trade_count"] == 2
        assert a_stat["win_rate"] == 0.5
