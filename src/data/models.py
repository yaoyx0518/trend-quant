from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from core.enums import SignalAction, SignalLevel


@dataclass(slots=True)
class InstrumentConfig:
    symbol: str
    enabled: bool
    risk_budget_pct: float
    stop_atr_mul: float


@dataclass(slots=True)
class SignalSnapshot:
    ts: datetime
    symbol: str
    action: SignalAction
    level: SignalLevel
    trend_score: float
    price_direction: float
    confidence: float
    reason: str
    calc_details: dict


@dataclass(slots=True)
class ManualTradeRecord:
    trade_date: str
    symbol: str
    side: str
    qty: int
    price: float
    fee: float
    trade_time: str
    note: str = ""
