from __future__ import annotations

from dataclasses import dataclass

from strategy.base import ExitDecision
from strategy.trend_score_core import safe_float


@dataclass(slots=True)
class HardStopExitRule:
    rule_id: str = "hard_stop"
    scope: str = "global"

    def evaluate(self, symbol: str, signal: dict, position: dict, state: dict, cfg: dict) -> ExitDecision:
        _ = symbol
        _ = position
        _ = cfg
        details = signal.get("calc_details", {})
        price = safe_float(details.get("price", 0.0), 0.0) if isinstance(details, dict) else 0.0
        hard_stop_price = safe_float(state.get("hard_stop_price", 0.0), 0.0)
        triggered = (price > 0) and (hard_stop_price > 0) and (price < hard_stop_price)
        return ExitDecision(
            triggered=triggered,
            reason=self.rule_id,
            scope=self.scope,
            meta={"hard_stop_price": hard_stop_price},
        )


@dataclass(slots=True)
class ChandelierStopExitRule:
    rule_id: str = "chandelier_stop"
    scope: str = "global"

    def evaluate(self, symbol: str, signal: dict, position: dict, state: dict, cfg: dict) -> ExitDecision:
        _ = symbol
        _ = position
        _ = cfg
        details = signal.get("calc_details", {})
        price = safe_float(details.get("price", 0.0), 0.0) if isinstance(details, dict) else 0.0
        chandelier_stop_price = safe_float(state.get("chandelier_stop_price", 0.0), 0.0)
        triggered = (price > 0) and (chandelier_stop_price > 0) and (price < chandelier_stop_price)
        return ExitDecision(
            triggered=triggered,
            reason=self.rule_id,
            scope=self.scope,
            meta={"chandelier_stop_price": chandelier_stop_price},
        )


def build_global_exit_rules() -> list[object]:
    return [HardStopExitRule(), ChandelierStopExitRule()]
