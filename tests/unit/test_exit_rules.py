"""Unit tests for strategy exit rules (strategy_exit_rules + global_exit_rules)."""

from __future__ import annotations

import pytest

from strategy.strategy_exit_rules import (
    TrendWeakenedExitRule,
    MABreakdownMaxExitRule,
    build_momentum_exit_rules,
)
from strategy.global_exit_rules import (
    HardStopExitRule,
    ChandelierStopExitRule,
    build_global_exit_rules,
)


class TestTrendWeakenedExitRule:
    def test_triggers_below_threshold(self) -> None:
        rule = TrendWeakenedExitRule()
        signal = {"trend_score": 3.0}
        cfg = {"exit_threshold": 5.0}
        decision = rule.evaluate("TEST", signal, {}, {}, cfg)
        assert decision.triggered is True

    def test_not_triggered_above_threshold(self) -> None:
        rule = TrendWeakenedExitRule()
        signal = {"trend_score": 8.0}
        cfg = {"exit_threshold": 5.0}
        decision = rule.evaluate("TEST", signal, {}, {}, cfg)
        assert decision.triggered is False

    def test_uses_default_exit_threshold(self) -> None:
        rule = TrendWeakenedExitRule()
        signal = {"trend_score": 4.0}
        decision = rule.evaluate("TEST", signal, {}, {}, {})
        assert decision.triggered is True  # 4 < default 5

    def test_scope_is_strategy(self) -> None:
        rule = TrendWeakenedExitRule()
        assert rule.scope == "strategy"


class TestMABreakdownMaxExitRule:
    def test_triggers_below_max_valid_ma(self) -> None:
        rule = MABreakdownMaxExitRule()
        signal = {"calc_details": {"price": 42.0, "ma30": 50.0, "ma40": 45.0}}
        decision = rule.evaluate("TEST", signal, {}, {}, {})
        # max valid MA = max(50 at 30, 45 at 40) = 50 at 30; price 42 < 50 → trigger
        assert decision.triggered is True
        assert "ma30_breakdown" in decision.reason

    def test_not_triggered_above_all_mas(self) -> None:
        rule = MABreakdownMaxExitRule()
        signal = {"calc_details": {"price": 55.0, "ma30": 50.0, "ma40": 45.0, "ma60": 40.0}}
        decision = rule.evaluate("TEST", signal, {}, {}, {})
        assert decision.triggered is False

    def test_no_valid_ma_returns_no_trigger(self) -> None:
        rule = MABreakdownMaxExitRule()
        signal = {"calc_details": {"price": 10.0, "ma30": 0.0, "ma40": 0.0, "ma60": 0.0}}
        decision = rule.evaluate("TEST", signal, {}, {}, {})
        assert decision.triggered is False


class TestBuildMomentumExitRules:
    def test_ma_breakdown_included(self) -> None:
        built = build_momentum_exit_rules(["ma_breakdown_max"])
        assert len(built) == 1
        assert isinstance(built[0], MABreakdownMaxExitRule)

    def test_unknown_signal_ignored(self) -> None:
        built = build_momentum_exit_rules(["unknown_signal"])
        assert built == []


class TestHardStopExitRule:
    def test_triggers_below_stop(self) -> None:
        rule = HardStopExitRule()
        signal = {"calc_details": {"price": 9.0}}
        state = {"hard_stop_price": 10.0}
        decision = rule.evaluate("TEST", signal, {}, state, {})
        assert decision.triggered is True

    def test_not_triggered_above_stop(self) -> None:
        rule = HardStopExitRule()
        signal = {"calc_details": {"price": 11.0}}
        state = {"hard_stop_price": 10.0}
        decision = rule.evaluate("TEST", signal, {}, state, {})
        assert decision.triggered is False

    def test_not_triggered_zero_stop(self) -> None:
        rule = HardStopExitRule()
        signal = {"calc_details": {"price": 9.0}}
        state = {"hard_stop_price": 0.0}
        decision = rule.evaluate("TEST", signal, {}, state, {})
        assert decision.triggered is False

    def test_scope_is_global(self) -> None:
        rule = HardStopExitRule()
        assert rule.scope == "global"


class TestChandelierStopExitRule:
    def test_triggers_below_stop(self) -> None:
        rule = ChandelierStopExitRule()
        signal = {"calc_details": {"price": 9.0}}
        state = {"chandelier_stop_price": 10.0}
        decision = rule.evaluate("TEST", signal, {}, state, {})
        assert decision.triggered is True

    def test_not_triggered_above_stop(self) -> None:
        rule = ChandelierStopExitRule()
        signal = {"calc_details": {"price": 11.0}}
        state = {"chandelier_stop_price": 10.0}
        decision = rule.evaluate("TEST", signal, {}, state, {})
        assert decision.triggered is False


class TestBuildGlobalExitRules:
    def test_returns_hard_stop_and_chandelier(self) -> None:
        rules = build_global_exit_rules()
        ids = {r.rule_id for r in rules}
        assert ids == {"hard_stop", "chandelier_stop"}
