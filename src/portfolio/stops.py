from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from core.enums import SignalAction, SignalLevel


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


class IStopRule(Protocol):
    name: str

    def is_triggered(self, symbol: str, price: float, position: dict, state: dict) -> bool: ...


@dataclass
class HardStopRule:
    name: str = "hard_stop"

    def is_triggered(self, symbol: str, price: float, position: dict, state: dict) -> bool:
        _ = symbol
        stop_price = _safe_float(
            state.get("hard_stop_price", position.get("hard_stop_price", 0.0)), 0.0
        )
        return (price > 0) and (stop_price > 0) and (price < stop_price)


@dataclass
class ChandelierStopRule:
    name: str = "chandelier_stop"

    def is_triggered(self, symbol: str, price: float, position: dict, state: dict) -> bool:
        _ = symbol
        stop_price = _safe_float(state.get("chandelier_stop_price", 0.0), 0.0)
        return (price > 0) and (stop_price > 0) and (price < stop_price)


class StopLossEngine:
    def __init__(self, rules: list[IStopRule] | None = None) -> None:
        self.rules: list[IStopRule] = list(rules or [HardStopRule(), ChandelierStopRule()])

    @staticmethod
    def _extract_price(signal: dict) -> float:
        details = signal.get("calc_details", {})
        if not isinstance(details, dict):
            return 0.0
        return _safe_float(details.get("price", 0.0), 0.0)

    def evaluate(self, symbol: str, signal: dict, position: dict, state: dict) -> list[str]:
        qty = int(position.get("qty", 0) or 0)
        if qty <= 0:
            return []
        price = self._extract_price(signal)
        if price <= 0:
            return []
        triggers: list[str] = []
        for rule in self.rules:
            if rule.is_triggered(symbol=symbol, price=price, position=position, state=state):
                triggers.append(rule.name)
        return triggers

    def enforce(self, symbol: str, signal: dict, position: dict, state: dict) -> list[str]:
        triggers = self.evaluate(symbol=symbol, signal=signal, position=position, state=state)
        if not triggers:
            return []

        sellable_qty = int(
            state.get("sellable_qty", position.get("sellable_qty", 0)) or 0
        )
        if sellable_qty > 0:
            signal["action"] = SignalAction.SELL
            signal["level"] = SignalLevel.ACTION
            signal["reason"] = "|".join(triggers)
        else:
            signal["action"] = SignalAction.HOLD
            signal["level"] = SignalLevel.WARN
            signal["reason"] = "t1_blocked:" + "|".join(triggers)

        details = signal.get("calc_details", {})
        if isinstance(details, dict):
            details["stop_triggers"] = triggers
            signal["calc_details"] = details
        return triggers
