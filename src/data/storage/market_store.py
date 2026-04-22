from __future__ import annotations

from pathlib import Path

import pandas as pd


class MarketStore:
    def __init__(self, base_dir: str = "data/market/etf") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def path_for(self, symbol: str) -> Path:
        safe_symbol = symbol.replace("/", "_")
        return self.base_dir / f"{safe_symbol}.parquet"

    def save_history(self, symbol: str, df: pd.DataFrame) -> Path:
        path = self.path_for(symbol)
        df.to_parquet(path, index=False)
        return path

    def load_history(self, symbol: str) -> pd.DataFrame:
        path = self.path_for(symbol)
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)
