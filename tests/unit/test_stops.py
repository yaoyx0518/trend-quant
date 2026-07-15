"""Unit tests for portfolio.stops — stop‑loss rules and engine."""

from __future__ import annotations

import pytest

from portfolio.stops import HardStopRule, ChandelierStopRule, StopLossEngine


class TestHardStopRule:
    def test_triggers_below_stop(self) -> None:
        rule = HardStopRule()
        assert rule.is_triggered("T", 9.0, {}, {"hard_stop_price": 10.0}) is True

    def test_not_triggered_above_stop(self) -> None:
        rule = HardStopRule()
        assert rule.is_triggered("T", 11.0, {}, {"hard_stop_price": 10.0}) is False

    def test_not_triggered_zero_stop(self) -> None:
        rule = HardStopRule()
        assert rule.is_triggered("T", 9.0, {}, {"hard_stop_price": 0.0}) is False

    def test_falls_back_to_position_stop(self) -> None:
        rule = HardStopRule()
        assert rule.is_triggered("T", 9.0, {"hard_stop_price": 10.0}, {}) is True


class TestChandelierStopRule:
    def test_triggers_below_stop(self) -> None:
        rule = ChandelierStopRule()
        assert rule.is_triggered("T", 9.0, {}, {"chandelier_stop_price": 10.0}) is True

    def test_not_triggered_above_stop(self) -> None:
        rule = ChandelierStopRule()
        assert rule.is_triggered("T", 11.0, {}, {"chandelier_stop_price": 10.0}) is False


class TestStopLossEngine:
    def test_evaluate_returns_trigger_names(self) -> None:
        engine = StopLossEngine([HardStopRule()])
        signal = {"calc_details": {"price": 9.0}}
        position = {"qty": 100}
        state = {"hard_stop_price": 10.0}
        triggers = engine.evaluate("T", signal, position, state)
        assert triggers == ["hard_stop"]

    def test_evaluate_no_position(self) -> None:
        engine = StopLossEngine()
        signal = {"calc_details": {"price": 9.0}}
        position = {"qty": 0}
        state = {"hard_stop_price": 10.0}
        assert engine.evaluate("T", signal, position, state) == []

    def test_evaluate_no_price(self) -> None:
        engine = StopLossEngine()
        signal = {"calc_details": {"price": 0.0}}
        position = {"qty": 100}
        state = {"hard_stop_price": 10.0}
        assert engine.evaluate("T", signal, position, state) == []

    def test_enforce_sets_sell_action(self) -> None:
        engine = StopLossEngine([HardStopRule()])
        signal: dict = {"calc_details": {"price": 9.0}}
        position = {"qty": 100}
        state = {"hard_stop_price": 10.0, "sellable_qty": 100}
        triggers = engine.enforce("T", signal, position, state)
        assert triggers == ["hard_stop"]
        assert signal["action"].value == "SELL"

    def test_enforce_t1_blocked_sets_warn(self) -> None:
        engine = StopLossEngine([HardStopRule()])
        signal: dict = {"calc_details": {"price": 9.0}}
        position = {"qty": 100}
        state = {"hard_stop_price": 10.0, "sellable_qty": 0}
        triggers = engine.enforce("T", signal, position, state)
        assert triggers == ["hard_stop"]
        assert signal["action"].value == "HOLD"
        assert signal["level"].value == "WARN"

    def test_default_rules(self) -> None:
        engine = StopLossEngine()
        assert len(engine.rules) == 2
        names = {r.name for r in engine.rules}
        assert names == {"hard_stop", "chandelier_stop"}
