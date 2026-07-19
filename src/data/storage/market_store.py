from __future__ import annotations

import pandas as pd


class MarketStore:
    def __init__(self, db=None, price_mode: str = "qfq") -> None:
        self._db = db
        self.price_mode = price_mode

    def _get_db(self):
        if self._db is None:
            from data.storage.db import get_db

            self._db = get_db()
        return self._db

    def save_history(self, symbol: str, df: pd.DataFrame) -> str:
        self._get_db().save_market_data(symbol, df, price_mode=self.price_mode)
        return f"sqlite/{self.price_mode}/{symbol}"

    def load_history(self, symbol: str) -> pd.DataFrame:
        return self._get_db().load_market_data(symbol, price_mode=self.price_mode)

    def list_stored_symbols(self) -> list[str]:
        return self._get_db().list_market_symbols(price_mode=self.price_mode)
