from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from audit.app_logger import get_logger
from audit.calc_logger import CalcLogger
from core.enums import SignalAction, SignalLevel
from data.service import DataService
from data.storage.db import get_db
from data.storage.runtime_store import RuntimeStore
from portfolio.risk_sizer import RiskSizer
from portfolio.service import PortfolioService
from strategy.indicators import atr
from strategy.trend_score_strategy import TrendScoreStrategy

logger = get_logger(__name__)


class SignalEngine:
    def __init__(self, provider_priority: list[str] | None = None, initial_capital: float = 200000.0) -> None:
        self.runtime_store = RuntimeStore()
        self.db = get_db()
        self.calc_logger = CalcLogger()
        self.data_service = DataService(provider_priority=provider_priority)
        self.strategy = TrendScoreStrategy()
        self.portfolio_service = PortfolioService(runtime_store=self.runtime_store)
        self.initial_capital = float(initial_capital)

    @staticmethod
    def _load_yaml(path: str) -> dict:
        with Path(path).open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def _load_configs(self) -> tuple[list[dict], dict]:
        instruments_cfg = self._load_yaml("config/instruments.yaml")
        strategy_cfg = self._load_yaml("config/strategy.yaml").get("strategy", {})
        instruments = [item for item in instruments_cfg.get("instruments", []) if item.get("enabled", True)]
        return instruments, strategy_cfg

    @staticmethod
    def _prepare_bars(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        bars = df.copy()
        if "time" in bars.columns:
            bars["time"] = pd.to_datetime(bars["time"], errors="coerce")
        for col in ("open", "high", "low", "close", "volume", "amount"):
            if col in bars.columns:
                bars[col] = pd.to_numeric(bars[col], errors="coerce")
        bars = bars.dropna(subset=["time", "close", "high", "low"]).sort_values("time").reset_index(drop=True)
        return bars

    def _ensure_symbol_bars(self, symbol: str, trade_day: date, adjust: str, lookback_days: int) -> pd.DataFrame:
        bars = self.data_service.market_store.load_history(symbol)
        bars = self._prepare_bars(bars)

        if bars.empty:
            start = trade_day - timedelta(days=max(lookback_days * 3, 180))
            bars = self.data_service.fetch_daily_history(symbol, start=start, end=trade_day, adjust=adjust)
            bars = self._prepare_bars(bars)
            if not bars.empty:
                self.data_service.market_store.save_history(symbol, bars)

        if bars.empty:
            return bars

        bars = bars.tail(max(lookback_days + 5, 80)).copy()

        latest_quote = self.data_service.fetch_latest_quote(symbol)
        latest_price = latest_quote.get("price")
        if latest_price is not None:
            idx = bars.index[-1]
            prev_close = float(bars.at[idx, "close"])
            latest_price_f = float(latest_price)

            # Ignore suspicious quote spikes to avoid contaminating signal state.
            if prev_close > 0:
                jump_ratio = abs(latest_price_f - prev_close) / prev_close
            else:
                jump_ratio = 0.0

            if latest_price_f > 0 and jump_ratio <= 0.30:
                bars.at[idx, "close"] = latest_price_f
                if latest_quote.get("open") is not None:
                    bars.at[idx, "open"] = float(latest_quote["open"])
                if latest_quote.get("high") is not None:
                    bars.at[idx, "high"] = float(latest_quote["high"])
                if latest_quote.get("low") is not None:
                    bars.at[idx, "low"] = float(latest_quote["low"])
                if latest_quote.get("volume") is not None:
                    bars.at[idx, "volume"] = float(latest_quote["volume"])
                if latest_quote.get("amount") is not None:
                    bars.at[idx, "amount"] = float(latest_quote["amount"])
            else:
                logger.warning(
                    "Skip abnormal latest quote for %s: latest=%s prev_close=%s",
                    symbol,
                    latest_price_f,
                    prev_close,
                )

        return bars

    @staticmethod
    def _derive_stops(position: dict, bars: pd.DataFrame, strategy_cfg: dict, symbol_cfg: dict) -> dict:
        qty = int(position.get("qty", 0) or 0)
        if qty <= 0 or bars.empty:
            return {"hard_stop_price": 0.0, "chandelier_stop_price": 0.0, "highest_price": 0.0, "atr": 0.0}

        atr_period = int(strategy_cfg.get("atr_period", 20))
        atr_series = atr(bars, period=atr_period)
        if atr_series.empty:
            return {"hard_stop_price": 0.0, "chandelier_stop_price": 0.0, "highest_price": 0.0, "atr": 0.0}

        current_atr = float(atr_series.iloc[-1]) if pd.notna(atr_series.iloc[-1]) else 0.0

        buy_date = position.get("buy_date")
        avg_price = float(position.get("avg_price", 0.0) or 0.0)
        stop_mul = float(symbol_cfg.get("stop_atr_mul", strategy_cfg.get("hard_stop_atr_mul_default", 1.5)))
        chandelier_mul = float(strategy_cfg.get("chandelier_stop_atr_mul", 2.5))

        bars_with_time = bars.copy()
        bars_with_time["time"] = pd.to_datetime(bars_with_time["time"], errors="coerce")

        atr_at_buy = current_atr
        highest_price = float(pd.to_numeric(bars_with_time["high"], errors="coerce").max())

        if buy_date:
            buy_ts = pd.Timestamp(buy_date)
            mask_until_buy = bars_with_time["time"] <= buy_ts
            buy_atr_series = atr_series[mask_until_buy]
            if not buy_atr_series.empty and pd.notna(buy_atr_series.iloc[-1]):
                atr_at_buy = float(buy_atr_series.iloc[-1])

            mask_since_buy = bars_with_time["time"] >= buy_ts
            highs = pd.to_numeric(bars_with_time.loc[mask_since_buy, "high"], errors="coerce")
            if not highs.empty and highs.notna().any():
                highest_price = float(highs.max())

        hard_stop_price = avg_price - stop_mul * atr_at_buy if avg_price > 0 and atr_at_buy > 0 else 0.0
        chandelier_stop_price = highest_price - chandelier_mul * current_atr if highest_price > 0 and current_atr > 0 else 0.0

        return {
            "hard_stop_price": hard_stop_price,
            "chandelier_stop_price": chandelier_stop_price,
            "highest_price": highest_price,
            "atr": current_atr,
        }

    @staticmethod
    def _estimate_buy_cost(qty: int, price: float, fee_rate: float, fee_min: float, slippage: float) -> float:
        if qty <= 0 or price <= 0:
            return 0.0
        deal_price = price * (1.0 + slippage)
        gross = qty * deal_price
        fee = max(gross * fee_rate, fee_min)
        return gross + fee

    @staticmethod
    def _action_value(action: object) -> str:
        if isinstance(action, SignalAction):
            return action.value
        text = str(action)
        if text.startswith("SignalAction."):
            return text.split(".", 1)[1]
        return text

    def is_trading_day(self, day: date | None = None) -> bool:
        today = day or date.today()
        return self.data_service.is_trading_day(today)

    def run_poll(self, trigger_name: str = "poll") -> dict:
        now = datetime.now()
        trade_day = now.date()
        if not self.is_trading_day(trade_day):
            payload = {
                "ts": now.isoformat(),
                "trigger": trigger_name,
                "signals": [],
                "status": "skipped_non_trading_day",
            }
            self.db.save_signals(trade_day.isoformat(), payload)
            return payload

        instruments, strategy_cfg = self._load_configs()
        lookback_days = int(strategy_cfg.get("lookback_days", 120))
        adjust = str(strategy_cfg.get("adjust", "qfq"))

        prev_state = self.db.get_all_signal_states() or {}
        current_state: dict = {}

        portfolio_snapshot = self.portfolio_service.build_snapshot(
            as_of_date=trade_day,
            initial_capital=self.initial_capital,
        )
        positions = portfolio_snapshot.get("positions", {})

        snapshots: list[dict] = []
        price_map: dict[str, float] = {}
        unavailable_symbols: list[str] = []

        for item in instruments:
            symbol = str(item.get("symbol"))
            bars = self._ensure_symbol_bars(symbol, trade_day, adjust, lookback_days)
            if bars.empty:
                unavailable_symbols.append(symbol)

            symbol_prev = prev_state.get(symbol, {}) if isinstance(prev_state, dict) else {}
            position = positions.get(symbol, {}) if isinstance(positions, dict) else {}
            stop_state = self._derive_stops(position, bars, strategy_cfg, item)

            strategy_state = {
                "prev_trend_score": float(symbol_prev.get("trend_score", 0.0)),
                "prev_prev_trend_score": float(symbol_prev.get("prev_trend_score", 0.0)),
                "position_qty": int(position.get("qty", 0)),
                "sellable_qty": int(position.get("sellable_qty", 0)),
                "hard_stop_price": float(stop_state.get("hard_stop_price", 0.0)),
                "chandelier_stop_price": float(stop_state.get("chandelier_stop_price", 0.0)),
            }

            signal = self.strategy.evaluate(symbol=symbol, bars=bars, state=strategy_state, cfg=strategy_cfg)
            signal["trigger"] = trigger_name
            signal["symbol_config"] = item
            signal["position_snapshot"] = {
                "qty": int(position.get("qty", 0)),
                "sellable_qty": int(position.get("sellable_qty", 0)),
                "avg_price": float(position.get("avg_price", 0.0) or 0.0),
                "buy_date": position.get("buy_date"),
            }
            signal["risk_state"] = stop_state

            snapshots.append(signal)
            self.calc_logger.log(signal)

            price_map[symbol] = float(signal.get("calc_details", {}).get("price", 0.0) or 0.0)
            current_state[symbol] = {
                "trend_score": signal.get("trend_score", 0.0),
                "prev_trend_score": float(symbol_prev.get("trend_score", 0.0)),
                "position_qty": int(position.get("qty", 0)),
                "updated_at": now.isoformat(),
            }

        cash = float(portfolio_snapshot.get("cash", self.initial_capital))
        equity = self.portfolio_service.estimate_equity(portfolio_snapshot, price_map)

        lot_size = int(self._load_yaml("config/app.yaml").get("app", {}).get("lot_size", 100))
        sizer = RiskSizer(lot_size=lot_size)

        slippage = float(strategy_cfg.get("slippage", 0.002))
        fee_rate = float(strategy_cfg.get("fee_rate", 0.000085))
        fee_min = float(strategy_cfg.get("fee_min", 5))
        max_position_vs_equal_weight = float(strategy_cfg.get("max_position_vs_equal_weight", 1.5))
        if max_position_vs_equal_weight <= 0:
            max_position_vs_equal_weight = 1.5
        asset_count = max(len(instruments), 1)
        max_position_ratio = max_position_vs_equal_weight / asset_count

        allocations: list[dict] = []
        signal_map = {str(sig.get("symbol")): sig for sig in snapshots}

        for item in instruments:
            symbol = str(item.get("symbol"))
            sig = signal_map.get(symbol)
            if sig is None:
                continue
            if self._action_value(sig.get("action")) != SignalAction.BUY.value:
                continue

            atr_value = float(sig.get("calc_details", {}).get("atr", 0.0) or 0.0)
            price = float(sig.get("calc_details", {}).get("price", 0.0) or 0.0)
            risk_budget_pct = float(item.get("risk_budget_pct", 0.01))
            stop_mul = float(item.get("stop_atr_mul", strategy_cfg.get("hard_stop_atr_mul_default", 1.5)))
            max_position_cost = max(equity, 0.0) * max_position_ratio

            raw_qty = sizer.suggest_qty(equity=equity, risk_budget_pct=risk_budget_pct, atr_value=atr_value, stop_mul=stop_mul)
            capped_qty = sizer.cap_qty_by_max_cost(
                qty=raw_qty,
                price=price,
                max_cost=max_position_cost,
                fee_rate=fee_rate,
                fee_min=fee_min,
                slippage=slippage,
            )
            qty = capped_qty
            cost = self._estimate_buy_cost(qty=qty, price=price, fee_rate=fee_rate, fee_min=fee_min, slippage=slippage)
            allocations.append(
                {
                    "symbol": symbol,
                    "qty": qty,
                    "raw_qty": raw_qty,
                    "capped_qty": capped_qty,
                    "max_position_ratio": max_position_ratio,
                    "max_position_cost": max_position_cost,
                    "cost": cost,
                    "price": price,
                }
            )

        scaled_allocs = sizer.scale_allocations(allocations, total_cash=max(cash, 0.0))
        scaled_map = {str(item.get("symbol")): item for item in scaled_allocs}

        for sig in snapshots:
            symbol = str(sig.get("symbol"))
            action = self._action_value(sig.get("action"))

            if action == SignalAction.BUY.value:
                alloc = scaled_map.get(symbol, {})
                suggested_qty = int(alloc.get("scaled_qty", 0))
                sig["suggested_qty"] = suggested_qty
                sig["sizing"] = {
                    "raw_qty": int(alloc.get("raw_qty", 0)),
                    "capped_qty": int(alloc.get("capped_qty", alloc.get("qty", 0))),
                    "scaled_qty": suggested_qty,
                    "scale_ratio": float(alloc.get("scale_ratio", 1.0)),
                    "estimated_cost": float(alloc.get("cost", 0.0)),
                    "max_position_ratio": float(alloc.get("max_position_ratio", max_position_ratio)),
                    "max_position_cost": float(alloc.get("max_position_cost", max(equity, 0.0) * max_position_ratio)),
                    "cash": cash,
                    "equity": equity,
                }
                if suggested_qty < lot_size:
                    sig["action"] = SignalAction.HOLD
                    sig["level"] = SignalLevel.WARN
                    sig["reason"] = "buy_insufficient_cash_or_risk"
            elif action == SignalAction.SELL.value:
                sellable = int(sig.get("position_snapshot", {}).get("sellable_qty", 0))
                sig["suggested_qty"] = max(sellable, 0)
                if sellable <= 0:
                    sig["action"] = SignalAction.HOLD
                    sig["level"] = SignalLevel.WARN
                    sig["reason"] = "sell_no_sellable_qty"
            else:
                sig["suggested_qty"] = 0

        status = "ok"
        if len(unavailable_symbols) == len(instruments) and len(instruments) > 0:
            status = "data_unavailable"
        elif unavailable_symbols:
            status = "partial_data_unavailable"

        payload = {
            "ts": now.isoformat(),
            "trigger": trigger_name,
            "signals": snapshots,
            "portfolio": {
                "cash": cash,
                "equity": equity,
                "position_count": len(positions),
            },
            "unavailable_symbols": unavailable_symbols,
            "status": status,
        }
        self.db.save_signals(trade_day.isoformat(), payload)
        self.db.save_signal_state(current_state)

        logger.info("Signal engine %s finished with %s symbols, status=%s", trigger_name, len(snapshots), status)
        return payload

    def run_daily_update(self) -> dict:
        today = date.today()
        if not self.is_trading_day(today):
            payload = {
                "ts": datetime.now().isoformat(),
                "status": "skipped_non_trading_day",
                "results": [],
            }
            self.runtime_store.write_json(f"advice/data_update_{today.isoformat()}.json", payload)
            return payload

        instruments, strategy_cfg = self._load_configs()
        symbols = [str(item.get("symbol")) for item in instruments]

        start_text = str(strategy_cfg.get("backtest_start_primary", "2015-01-01"))
        start_date = datetime.strptime(start_text, "%Y-%m-%d").date()
        payload = self.data_service.update_pool_daily(
            symbols=symbols,
            start_date=start_date,
            end_date=today,
            adjust=str(strategy_cfg.get("adjust", "qfq")),
        )
        logger.info("Daily update finished for %s symbols", len(symbols))
        return payload
