from __future__ import annotations

import pandas as pd

from rule_backtest.models import PositionState
from rule_backtest.value_resolver import ValueResolver


class ConditionEngine:
    def __init__(self, resolver: ValueResolver) -> None:
        self.resolver = resolver

    def evaluate_group(
        self,
        group: dict,
        bars: pd.DataFrame,
        position: PositionState,
        debug: bool = False,
        combinator: str | None = None,
    ) -> tuple[bool, list[dict]]:
        if not isinstance(group, dict):
            return False, []
        children = group.get("children", []) or []
        combinator = combinator or str(group.get("combinator", "all")).strip().lower()
        traces: list[dict] = []
        results: list[bool] = []
        for idx, child in enumerate(children):
            passed, trace = self.evaluate_condition(child, bars=bars, position=position, debug=debug)
            trace["condition_index"] = idx
            traces.append(trace)
            results.append(passed)

        if not results:
            return False, traces
        if combinator == "any":
            return any(results), traces
        return all(results), traces

    def evaluate_condition(
        self,
        condition: dict,
        bars: pd.DataFrame,
        position: PositionState,
        debug: bool = False,
    ) -> tuple[bool, dict]:
        left_value, left_trace = self.resolver.resolve(condition.get("left", {}) or {}, bars, position, debug=debug)
        right_value, right_trace = self.resolver.resolve(condition.get("right", {}) or {}, bars, position, debug=debug)
        operator = str(condition.get("operator", "")).strip()

        passed = False
        if left_value is not None and right_value is not None:
            if operator == ">=":
                passed = float(left_value) >= float(right_value)
            elif operator == "<=":
                passed = float(left_value) <= float(right_value)

        trace = {
            "condition_id": condition.get("id"),
            "operator": operator,
            "left_value": left_value,
            "right_value": right_value,
            "passed": passed,
        }
        if debug:
            trace["left_trace"] = left_trace
            trace["right_trace"] = right_trace
        return passed, trace
