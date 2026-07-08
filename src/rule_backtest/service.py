from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import re

import pandas as pd
import yaml

from data.storage.market_store import MarketStore
from rule_backtest.engine import SingleSymbolAllInBacktestEngine
from rule_backtest.loader import StrategyLoader
from rule_backtest.models import BacktestExecutionConfig, RuleBacktestRequest
from rule_backtest.registry import registry_payload


class RuleBacktestService:
    def __init__(
        self,
        strategy_loader: StrategyLoader | None = None,
        market_store: MarketStore | None = None,
    ) -> None:
        self.strategy_loader = strategy_loader or StrategyLoader()
        self.market_store = market_store or MarketStore()
        self.engine = SingleSymbolAllInBacktestEngine()

    def list_strategies(self) -> list[dict]:
        return self.strategy_loader.list_strategies()

    def list_indicators(self) -> list[dict]:
        return registry_payload()

    def save_strategy(self, strategy: dict, overwrite: bool = False) -> dict:
        if not str(strategy.get("id", "")).strip():
            strategy = dict(strategy)
            strategy["id"] = self._generate_strategy_id(str(strategy.get("name", "") or "strategy"))
        return self.strategy_loader.save(strategy=strategy, overwrite=overwrite)

    def list_instruments(self) -> list[dict]:
        path = Path("config/instruments.yaml")
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8") as f:
            payload = yaml.safe_load(f) or {}
        instruments = payload.get("instruments", [])
        if not isinstance(instruments, list):
            return []
        rows: list[dict] = []
        try:
            stored_symbols = set(self.market_store.list_stored_symbols())
        except Exception:
            stored_symbols = set()
        for item in instruments:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "name": str(item.get("name", "") or ""),
                    "enabled": bool(item.get("enabled", True)),
                    "asset_type": str(item.get("asset_type", "etf") or "etf"),
                    "has_market_data": symbol in stored_symbols,
                }
            )
        return rows

    def run(self, payload: dict) -> dict:
        strategy_id = str(payload.get("strategy_id", "")).strip()
        symbol = str(payload.get("symbol", "")).strip().upper()
        if not strategy_id:
            raise ValueError("strategy_id is required")
        if not symbol:
            raise ValueError("symbol is required")

        strategy = self.strategy_loader.load(strategy_id)
        bars = self.market_store.load_history(symbol)
        bars = self._filter_bars(
            bars=bars,
            start_date=self._parse_date(payload.get("start_date")),
            end_date=self._parse_date(payload.get("end_date")),
        )
        if bars.empty:
            raise ValueError(f"symbol has no market data in range: {symbol}")

        instrument_type = str(payload.get("instrument_type", "") or "").strip().lower()
        if instrument_type not in {"etf", "stock"}:
            instrument_type = self._resolve_instrument_type(symbol)

        execution = BacktestExecutionConfig(
            initial_capital=float(payload.get("initial_capital", 1_000_000.0) or 1_000_000.0),
            fee_rate=float(payload.get("fee_rate", 0.0000854) or 0.0000854),
            fee_min=float(payload.get("fee_min", 5.0) or 5.0),
            slippage=float(payload.get("slippage", 0.002) or 0.002),
            lot_size=int(payload.get("lot_size", 100) or 100),
            instrument_type="stock" if instrument_type == "stock" else "etf",
            stock_stamp_tax_rate=float(payload.get("stock_stamp_tax_rate", 0.001) or 0.001),
            debug_log_enabled=self._parse_debug_flag(payload.get("debug_log_enabled")),
        )
        request = RuleBacktestRequest(
            strategy=strategy,
            symbol=symbol,
            bars=bars,
            execution=execution,
            run_id=datetime.now().strftime("%Y%m%d%H%M%S%f"),
        )
        return self.engine.run(request)

    @staticmethod
    def _parse_date(value: object) -> date | None:
        text = str(value or "").strip()
        if not text:
            return None
        return datetime.strptime(text, "%Y-%m-%d").date()

    @staticmethod
    def _filter_bars(bars: pd.DataFrame, start_date: date | None, end_date: date | None) -> pd.DataFrame:
        if bars.empty:
            return bars
        df = bars.copy()
        if "date" not in df.columns and "time" in df.columns:
            df["date"] = pd.to_datetime(df["time"], errors="coerce").dt.date
        else:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        if start_date is not None:
            df = df[df["date"] >= start_date]
        if end_date is not None:
            df = df[df["date"] <= end_date]
        return df

    def _resolve_instrument_type(self, symbol: str) -> str:
        for item in self.list_instruments():
            if item["symbol"] == symbol:
                asset_type = str(item.get("asset_type", "etf")).lower()
                return "stock" if asset_type == "stock" else "etf"
        return "etf"

    @staticmethod
    def _parse_debug_flag(value: object) -> bool | None:
        text = str(value or "").strip().lower()
        if text == "":
            return None
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return None

    @staticmethod
    def _generate_strategy_id(name: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")[:32]
        suffix = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"{slug or 'strategy'}_{suffix}"
