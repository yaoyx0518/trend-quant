from __future__ import annotations

import pandas as pd

from rule_backtest.indicators import atr, latest_field
from rule_backtest.models import PositionState


def update_position_state_for_day(
    position: PositionState,
    bars: pd.DataFrame,
    strategy: dict,
) -> dict:
    if not position.is_open:
        return {}

    high = latest_field(bars, "high")
    if high is not None:
        if position.highest_high_since_entry <= 0:
            position.highest_high_since_entry = float(high)
        else:
            position.highest_high_since_entry = max(position.highest_high_since_entry, float(high))

    trace: dict = {"highest_high_since_entry": position.highest_high_since_entry}
    exit_group = strategy.get("exit", {}) if isinstance(strategy.get("exit", {}), dict) else {}
    for condition in exit_group.get("children", []) or []:
        for side in ("left", "right"):
            spec = condition.get(side, {}) if isinstance(condition, dict) else {}
            if not isinstance(spec, dict) or spec.get("type") != "state_value":
                continue
            if spec.get("name") != "chandelier_stop":
                continue
            params = spec.get("params", {}) if isinstance(spec.get("params", {}), dict) else {}
            atr_period = int(params.get("atr_period", 20))
            atr_mul = float(params.get("atr_mul", 2.5))
            atr_value, atr_trace = atr(bars, period=atr_period)
            if atr_value is not None and position.highest_high_since_entry > 0:
                position.chandelier_stop = position.highest_high_since_entry - atr_mul * atr_value
            trace["chandelier_stop"] = position.chandelier_stop
            trace["chandelier_atr"] = atr_trace
    return trace


def initialize_stop_state(
    position: PositionState,
    bars: pd.DataFrame,
    strategy: dict,
    entry_price: float,
    entry_date: str,
) -> dict:
    position.entry_price = float(entry_price)
    position.entry_date = entry_date
    high = latest_field(bars, "high")
    position.highest_high_since_entry = float(high if high is not None else entry_price)

    trace: dict = {"entry_price": entry_price, "entry_date": entry_date}
    exit_group = strategy.get("exit", {}) if isinstance(strategy.get("exit", {}), dict) else {}
    for condition in exit_group.get("children", []) or []:
        for side in ("left", "right"):
            spec = condition.get(side, {}) if isinstance(condition, dict) else {}
            if not isinstance(spec, dict) or spec.get("type") != "state_value":
                continue
            params = spec.get("params", {}) if isinstance(spec.get("params", {}), dict) else {}
            name = str(spec.get("name", "")).strip()
            if name == "hard_stop":
                atr_period = int(params.get("atr_period", 20))
                atr_mul = float(params.get("atr_mul", 1.5))
                atr_value, atr_trace = atr(bars, period=atr_period)
                position.atr_at_entry = float(atr_value or 0.0)
                position.hard_stop = entry_price - atr_mul * position.atr_at_entry if atr_value is not None else 0.0
                trace["hard_stop"] = position.hard_stop
                trace["hard_stop_atr"] = atr_trace
            elif name == "chandelier_stop":
                atr_period = int(params.get("atr_period", 20))
                atr_mul = float(params.get("atr_mul", 2.5))
                atr_value, atr_trace = atr(bars, period=atr_period)
                if atr_value is not None:
                    position.chandelier_stop = position.highest_high_since_entry - atr_mul * atr_value
                trace["chandelier_stop"] = position.chandelier_stop
                trace["chandelier_atr"] = atr_trace
    return trace
