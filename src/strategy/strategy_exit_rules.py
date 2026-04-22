from __future__ import annotations

from dataclasses import dataclass

from strategy.base import ExitDecision
from strategy.momentum_signal_modules import SELL_SIGNAL_MA_BREAKDOWN_MAX
from strategy.trend_score_core import safe_float


@dataclass(slots=True)
class TrendWeakenedExitRule:
    rule_id: str = "trend_weakened"
    scope: str = "strategy"

    def evaluate(self, symbol: str, signal: dict, position: dict, state: dict, cfg: dict) -> ExitDecision:
        _ = symbol
        _ = position
        _ = state
        exit_threshold = safe_float(cfg.get("exit_threshold", 5.0), 5.0)
        trend_score = safe_float(signal.get("trend_score", 0.0), 0.0)
        return ExitDecision(
            triggered=trend_score < exit_threshold,
            reason=self.rule_id,
            scope=self.scope,
            meta={"exit_threshold": exit_threshold},
        )


@dataclass(slots=True)
class MABreakdownMaxExitRule:
    rule_id: str = SELL_SIGNAL_MA_BREAKDOWN_MAX
    scope: str = "strategy"

    def evaluate(self, symbol: str, signal: dict, position: dict, state: dict, cfg: dict) -> ExitDecision:
        _ = symbol
        _ = position
        _ = state
        details = signal.get("calc_details", {})
        if not isinstance(details, dict):
            details = {}

        price = safe_float(details.get("price", 0.0), 0.0)
        ma_candidates = {
            30: safe_float(details.get("ma30", 0.0), 0.0),
            40: safe_float(details.get("ma40", 0.0), 0.0),
            60: safe_float(details.get("ma60", 0.0), 0.0),
        }
        valid_ma_candidates = {period: value for period, value in ma_candidates.items() if value > 0}
        if not valid_ma_candidates:
            return ExitDecision(triggered=False, reason="", scope=self.scope, meta={})

        exit_ma_period = max(valid_ma_candidates, key=valid_ma_candidates.get)
        exit_ma_value = float(valid_ma_candidates.get(exit_ma_period, 0.0))
        triggered = (price > 0) and (exit_ma_value > 0) and (price < exit_ma_value)
        return ExitDecision(
            triggered=triggered,
            reason=f"ma{exit_ma_period}_breakdown_exit",
            scope=self.scope,
            meta={
                "exit_ma_candidates": valid_ma_candidates,
                "exit_ma_period": int(exit_ma_period),
                "exit_ma_value": exit_ma_value,
            },
        )


def build_momentum_exit_rules(signal_ids: list[str]) -> list[object]:
    built: list[object] = []
    for signal_id in signal_ids:
        if signal_id == SELL_SIGNAL_MA_BREAKDOWN_MAX:
            built.append(MABreakdownMaxExitRule())
    return built
