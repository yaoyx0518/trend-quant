"""Unit tests for rule_backtest.metrics — rule‑backtest compute functions."""

from __future__ import annotations

import pytest

from rule_backtest.metrics import (
    compute_drawdown,
    compute_summary,
    annual_returns,
    monthly_returns,
)


def _make_nav(equities: list[float], start_date: str = "2025-01-02") -> list[dict]:
    """helper: create daily_nav list from equity values."""
    from datetime import date, timedelta

    base = date.fromisoformat(start_date)
    return [{"date": (base + timedelta(days=i)).isoformat(), "equity": e}
            for i, e in enumerate(equities)]


class TestComputeDrawdown:
    def test_no_drawdown(self) -> None:
        """rule_backtest.metrics.compute_drawdown returns list of dicts."""
        nav = _make_nav([100_000, 101_000, 102_000])
        dd = compute_drawdown(nav)
        assert len(dd) == 3
        for item in dd:
            assert item["drawdown"] == 0.0

    def test_with_drawdown(self) -> None:
        nav = _make_nav([100_000, 80_000, 90_000])
        dd = compute_drawdown(nav)
        assert dd[1]["drawdown"] == pytest.approx(-0.2, abs=0.01)


class TestComputeSummary:
    def test_returns_required_keys(self) -> None:
        nav = _make_nav([100_000, 110_000, 105_000])
        summary = compute_summary(nav, [], 0.0)
        for key in ("total_return", "annual_return", "max_drawdown", "sharpe",
                     "sortino", "calmar", "win_rate", "profit_factor", "trade_count"):
            assert key in summary

    def test_no_trades_zero_win_rate(self) -> None:
        nav = _make_nav([100_000, 110_000])
        summary = compute_summary(nav, [], 0.0)
        assert summary["win_rate"] == 0.0
        assert summary["trade_count"] == 0


class TestAnnualReturns:
    def test_returns_year_list(self) -> None:
        nav = _make_nav([100_000, 120_000, 110_000] * 100)
        result = annual_returns(nav)
        assert len(result) >= 1
        assert "year" in result[0]


class TestMonthlyReturns:
    def test_returns_monthly_data(self) -> None:
        nav = _make_nav([100_000, 105_000] * 15)
        result = monthly_returns(nav)
        assert isinstance(result, list)
