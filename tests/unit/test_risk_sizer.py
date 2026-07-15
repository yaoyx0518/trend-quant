"""Unit tests for portfolio.risk_sizer.RiskSizer."""

from __future__ import annotations

import pytest

from portfolio.risk_sizer import RiskSizer


class TestSuggestQty:
    def test_basic_sizing(self) -> None:
        sizer = RiskSizer(lot_size=100)
        qty = sizer.suggest_qty(equity=200_000, risk_budget_pct=0.01, atr_value=0.5, stop_mul=2.0)
        # per_share_risk = 0.5*2 = 1.0
        # raw = 200000*0.01/1.0 = 2000
        # lots = 2000//100 = 20 → qty = 2000
        assert qty == 2000

    def test_zero_atr_returns_zero(self) -> None:
        sizer = RiskSizer()
        assert sizer.suggest_qty(100_000, 0.01, 0.0, 2.0) == 0

    def test_zero_stop_mul_returns_zero(self) -> None:
        sizer = RiskSizer()
        assert sizer.suggest_qty(100_000, 0.01, 1.0, 0.0) == 0

    def test_zero_equity_returns_zero(self) -> None:
        sizer = RiskSizer()
        assert sizer.suggest_qty(0, 0.01, 1.0, 2.0) == 0

    def test_rounds_to_lot_size(self) -> None:
        sizer = RiskSizer(lot_size=10)
        qty = sizer.suggest_qty(100_000, 0.01, 10.0, 1.0)
        # per_share_risk=10, raw=100, lots=10→qty=100. Must be multiple of 10.
        assert qty % 10 == 0


class TestCapQtyByMaxCost:
    def test_within_budget(self) -> None:
        sizer = RiskSizer(lot_size=100)
        capped = sizer.cap_qty_by_max_cost(500, 10.0, 6_000)
        assert capped == 500

    def test_exceeds_budget(self) -> None:
        sizer = RiskSizer(lot_size=100)
        capped = sizer.cap_qty_by_max_cost(1_000, 10.0, 5_000)
        assert capped < 1_000

    def test_zero_qty(self) -> None:
        sizer = RiskSizer()
        assert sizer.cap_qty_by_max_cost(0, 10.0, 10_000) == 0

    def test_zero_price(self) -> None:
        sizer = RiskSizer()
        assert sizer.cap_qty_by_max_cost(100, 0, 10_000) == 0

    def test_zero_max_cost(self) -> None:
        sizer = RiskSizer()
        assert sizer.cap_qty_by_max_cost(100, 10.0, 0) == 0


class TestScaleAllocations:
    def test_within_total_cash(self) -> None:
        sizer = RiskSizer(lot_size=100)
        allocs = [{"qty": 100, "cost": 1000.0}]
        result = sizer.scale_allocations(allocs, 5_000)
        assert result[0]["scaled_qty"] == 100
        assert result[0]["scale_ratio"] == 1.0

    def test_exceeds_total_cash(self) -> None:
        sizer = RiskSizer(lot_size=100)
        allocs = [{"qty": 1_000, "cost": 20_000.0}]
        result = sizer.scale_allocations(allocs, 10_000)
        assert result[0]["scaled_qty"] < 1_000
        assert result[0]["scale_ratio"] < 1.0
