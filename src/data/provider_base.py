from __future__ import annotations

from datetime import date
from typing import Protocol

import pandas as pd


class IDataProvider(Protocol):
    name: str

    def fetch_daily_history(self, symbol: str, start: date, end: date, adjust: str) -> pd.DataFrame: ...
    def fetch_minute_history(self, symbol: str, period: str, count: int, adjust: str) -> pd.DataFrame: ...
    def fetch_latest_quote(self, symbol: str) -> dict: ...
    def fetch_trading_calendar(self, start: date, end: date) -> list[date]: ...
