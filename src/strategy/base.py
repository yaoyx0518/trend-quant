from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol

import pandas as pd

from core.enums import SignalAction, SignalLevel


@dataclass(frozen=True, slots=True)
class ExitDecision:
    triggered: bool
    reason: str
    scope: str
    meta: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EntryDecision:
    triggered: bool
    reason: str
    meta: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FilterDecision:
    passed: bool
    reason: str
    filter_id: str
    meta: dict[str, object] = field(default_factory=dict)


class ExitRule(Protocol):
    rule_id: str
    scope: str

    def evaluate(
        self,
        symbol: str,
        signal: dict,
        position: dict,
        state: dict,
        cfg: dict,
    ) -> ExitDecision: ...


class EntryFilter(Protocol):
    filter_id: str

    def evaluate(self, signal: dict, cfg: dict) -> FilterDecision: ...


class CrossSectionPlanner(Protocol):
    planner_id: str

    def plan(
        self,
        day: date,
        signal_map: dict[str, dict],
        positions: dict[str, dict],
        cfg: dict,
    ) -> dict: ...


class IStrategy(Protocol):
    name: str

    def evaluate(self, symbol: str, bars: pd.DataFrame, state: dict, cfg: dict) -> dict: ...

    def finalize_day(
        self,
        day: date,
        signal_map: dict[str, dict],
        positions: dict[str, dict],
        cfg: dict,
    ) -> dict: ...

    def required_history_bars(self, cfg: dict) -> int: ...


def action_value(action: object) -> str:
    if isinstance(action, SignalAction):
        return action.value
    text = str(action)
    if text.startswith("SignalAction."):
        return text.split(".", 1)[1]
    return text


def serialize_exit_decision(decision: ExitDecision) -> dict[str, object]:
    return {
        "triggered": bool(decision.triggered),
        "reason": str(decision.reason),
        "scope": str(decision.scope),
        "meta": dict(decision.meta or {}),
    }


def append_exit_decisions(signal: dict, decisions: list[ExitDecision]) -> None:
    details = signal.get("calc_details", {})
    if not isinstance(details, dict):
        details = {}
    existing = details.get("exit_decisions", [])
    if not isinstance(existing, list):
        existing = []
    existing.extend(serialize_exit_decision(decision) for decision in decisions)
    details["exit_decisions"] = existing
    signal["calc_details"] = details


def apply_exit_decisions(signal: dict, state: dict, decisions: list[ExitDecision]) -> bool:
    triggered = [decision for decision in decisions if decision.triggered and decision.reason]
    if not triggered:
        return False

    append_exit_decisions(signal, decisions)

    details = signal.get("calc_details", {})
    if not isinstance(details, dict):
        details = {}

    global_reasons = [decision.reason for decision in triggered if decision.scope == "global"]
    strategy_reasons = [decision.reason for decision in triggered if decision.scope == "strategy"]
    if global_reasons:
        details["stop_triggers"] = global_reasons
    if strategy_reasons:
        details["strategy_exit_triggers"] = strategy_reasons

    for decision in triggered:
        meta = decision.meta or {}
        if isinstance(meta, dict):
            details.update(meta)

    sellable_qty = int(state.get("sellable_qty", 0) or 0)
    reasons = [decision.reason for decision in triggered]
    if sellable_qty > 0:
        signal["action"] = SignalAction.SELL
        signal["level"] = SignalLevel.ACTION
        signal["reason"] = "|".join(reasons)
    else:
        signal["action"] = SignalAction.HOLD
        signal["level"] = SignalLevel.WARN
        signal["reason"] = "t1_blocked:" + "|".join(reasons)

    signal["calc_details"] = details
    return True


class BaseStrategy(ABC):
    name = "base_strategy"

    @abstractmethod
    def compute_features(self, symbol: str, bars: pd.DataFrame, state: dict, cfg: dict) -> dict:
        raise NotImplementedError

    def get_global_exit_rules(self, cfg: dict) -> list[ExitRule]:
        _ = cfg
        from strategy.global_exit_rules import build_global_exit_rules

        return build_global_exit_rules()

    def get_strategy_exit_rules(self, cfg: dict) -> list[ExitRule]:
        _ = cfg
        return []

    def get_entry_filters(self, cfg: dict) -> list[EntryFilter]:
        _ = cfg
        return []

    def get_cross_section_planner(self, cfg: dict) -> CrossSectionPlanner | None:
        _ = cfg
        return None

    def evaluate_entry_signal(self, signal: dict, state: dict, cfg: dict) -> EntryDecision:
        _ = signal
        _ = state
        _ = cfg
        return EntryDecision(triggered=False, reason="")

    def default_hold_reason(self, signal: dict, state: dict, cfg: dict) -> str:
        _ = state
        _ = cfg
        if bool(signal.get("ok", False)):
            return "scored"
        return str(signal.get("reason", "invalid_signal"))

    def decorate_signal(self, signal: dict, state: dict, cfg: dict) -> dict:
        _ = state
        _ = cfg
        return signal

    def required_history_bars(self, cfg: dict) -> int:
        lookback = int(cfg.get("lookback_days", 120))
        n_long = int(cfg.get("n_long", 40))
        atr_period = int(cfg.get("atr_period", 20))
        return max(lookback, n_long, atr_period, 40)

    def finalize_day(
        self,
        day: date,
        signal_map: dict[str, dict],
        positions: dict[str, dict],
        cfg: dict,
    ) -> dict:
        planner = self.get_cross_section_planner(cfg)
        if planner is None:
            return {
                "is_rebalance_day": False,
                "planned_holdings": [],
                "to_buy": [],
                "to_sell": [],
            }
        return planner.plan(day=day, signal_map=signal_map, positions=positions, cfg=cfg)

    @staticmethod
    def _signal_from_features(symbol: str, features: dict) -> dict:
        calc_details = features.get("calc_details", {})
        if not isinstance(calc_details, dict):
            calc_details = {}

        signal = {
            "symbol": symbol,
            "action": SignalAction.HOLD,
            "level": SignalLevel.INFO if bool(features.get("ok", False)) else SignalLevel.WARN,
            "reason": str(features.get("reason", "scored")),
            "calc_details": dict(calc_details),
            "ts": datetime.now().isoformat(),
        }
        for key, value in features.items():
            if key == "calc_details":
                continue
            signal[key] = value
        return signal

    def evaluate(self, symbol: str, bars: pd.DataFrame, state: dict, cfg: dict) -> dict:
        features = self.compute_features(symbol=symbol, bars=bars, state=state, cfg=cfg)
        signal = self._signal_from_features(symbol=symbol, features=features)

        details = signal.get("calc_details", {})
        if not isinstance(details, dict):
            details = {}

        default_reason = self.default_hold_reason(signal=signal, state=state, cfg=cfg)
        signal["reason"] = default_reason

        if not bool(features.get("ok", False)):
            signal["calc_details"] = details
            return self.decorate_signal(signal=signal, state=state, cfg=cfg)

        position_qty = int(state.get("position_qty", 0) or 0)
        entry_candidate = False
        entry_passed: bool | None = None

        global_exit_rules = self.get_global_exit_rules(cfg)
        global_decisions = [
            rule.evaluate(symbol=symbol, signal=signal, position=state, state=state, cfg=cfg)
            for rule in global_exit_rules
        ]

        if position_qty > 0:
            if apply_exit_decisions(signal=signal, state=state, decisions=global_decisions):
                signal["entry_candidate"] = False
                signal["entry_passed"] = None
                return self.decorate_signal(signal=signal, state=state, cfg=cfg)

            strategy_exit_rules = self.get_strategy_exit_rules(cfg)
            strategy_decisions = [
                rule.evaluate(symbol=symbol, signal=signal, position=state, state=state, cfg=cfg)
                for rule in strategy_exit_rules
            ]
            if apply_exit_decisions(signal=signal, state=state, decisions=strategy_decisions):
                signal["entry_candidate"] = False
                signal["entry_passed"] = None
                return self.decorate_signal(signal=signal, state=state, cfg=cfg)

            append_exit_decisions(signal, global_decisions)
        else:
            append_exit_decisions(signal, global_decisions)

            entry_decision = self.evaluate_entry_signal(signal=signal, state=state, cfg=cfg)
            details = signal.get("calc_details", {})
            if not isinstance(details, dict):
                details = {}
            details["entry_decision"] = {
                "triggered": bool(entry_decision.triggered),
                "reason": str(entry_decision.reason),
                "meta": dict(entry_decision.meta or {}),
            }

            if entry_decision.triggered:
                filters = self.get_entry_filters(cfg)
                filter_decisions = [flt.evaluate(signal=signal, cfg=cfg) for flt in filters]
                filter_results = {decision.filter_id: bool(decision.passed) for decision in filter_decisions}
                details["entry_filters_enabled"] = [decision.filter_id for decision in filter_decisions]
                details["entry_filter_results"] = filter_results
                for decision in filter_decisions:
                    if isinstance(decision.meta, dict):
                        details.update(decision.meta)

                entry_candidate = True
                entry_passed = all(decision.passed for decision in filter_decisions)
                signal["entry_candidate"] = entry_candidate
                signal["entry_passed"] = entry_passed

                if entry_passed:
                    planner = self.get_cross_section_planner(cfg)
                    if planner is None:
                        signal["action"] = SignalAction.BUY
                        signal["level"] = SignalLevel.ACTION
                    signal["reason"] = entry_decision.reason or "entry_triggered"
                else:
                    signal["level"] = SignalLevel.WARN
                    signal["reason"] = next(
                        (decision.reason for decision in filter_decisions if not decision.passed and decision.reason),
                        "entry_filtered",
                    )
            else:
                entry_passed = False
                signal["entry_candidate"] = False
                signal["entry_passed"] = False

            signal["calc_details"] = details

        signal["entry_candidate"] = entry_candidate
        signal["entry_passed"] = entry_passed
        return self.decorate_signal(signal=signal, state=state, cfg=cfg)
