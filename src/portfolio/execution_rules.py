from __future__ import annotations

from datetime import date


def can_sell_t1(buy_date: str | None, trade_day: date) -> bool:
    if not buy_date:
        return True
    return buy_date < trade_day.isoformat()
