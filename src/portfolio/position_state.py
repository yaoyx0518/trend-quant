from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(slots=True)
class PositionState:
    symbol: str
    qty: int = 0
    avg_price: float = 0.0
    highest_price: float = 0.0
    hard_stop_price: float = 0.0
    buy_date: str | None = None
    sellable_qty: int = 0

    def to_dict(self) -> dict:
        return asdict(self)
