from __future__ import annotations

from pathlib import Path

import pandas as pd


class MarketStore:
    def __init__(self, db=None) -> None:
        self._db = db

    def _get_db(self):
        if self._db is None:
            from data.storage.db import get_db
            self._db = get_db()
        return self._db

    def save_history(self, symbol: str, df: pd.DataFrame) -> str:
        self._get_db().save_market_data(symbol, df)
        return f"sqlite/{symbol}"

    def load_history(self, symbol: str) -> pd.DataFrame:
        return self._get_db().load_market_data(symbol)

    def list_stored_symbols(self) -> list[str]:
        return self._get_db().list_market_symbols()

    @property
    def base_dir(self) -> Path:
        return Path("data/market/etf")

    def path_for(self, symbol: str) -> Path:
        safe_symbol = symbol.replace("/", "_")
        return self.base_dir / f"{safe_symbol}.parquet"
