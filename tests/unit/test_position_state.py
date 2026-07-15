"""Unit tests for portfolio.position_state.PositionState."""

from __future__ import annotations

from portfolio.position_state import PositionState


class TestPositionState:
    def test_default_values(self) -> None:
        ps = PositionState(symbol="TEST")
        assert ps.symbol == "TEST"
        assert ps.qty == 0
        assert ps.avg_price == 0.0
        assert ps.highest_price == 0.0
        assert ps.hard_stop_price == 0.0
        assert ps.buy_date is None
        assert ps.sellable_qty == 0

    def test_to_dict_roundtrip(self) -> None:
        ps = PositionState(
            symbol="510300.SS",
            qty=500,
            avg_price=3.80,
            highest_price=4.20,
            hard_stop_price=3.50,
            buy_date="2025-08-11",
            sellable_qty=500,
        )
        d = ps.to_dict()
        assert d["symbol"] == "510300.SS"
        assert d["qty"] == 500
        assert d["avg_price"] == 3.80

    def test_field_types(self) -> None:
        ps = PositionState(symbol="TEST", qty=100)
        assert isinstance(ps.qty, int)
        assert isinstance(ps.avg_price, float)
        assert isinstance(ps.sellable_qty, int)
