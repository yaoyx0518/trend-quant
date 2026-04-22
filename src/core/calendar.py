from __future__ import annotations

from datetime import date


def is_trading_day(day: date) -> bool:
    """Minimal V1 trading-day check (Mon-Fri).

    TODO(V1.1): Replace with full A-share holiday calendar provider.
    """
    return day.weekday() < 5
