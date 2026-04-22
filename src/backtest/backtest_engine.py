from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import yaml

from backtest.benchmark import equal_weight_pool_benchmark, single_symbol_benchmark
from backtest.metrics import compute_annual_returns, compute_drawdown, compute_metrics, compute_monthly_heatmap, compute_symbol_trade_stats
from core.enums import SignalAction
from data.storage.db import get_db
from data.storage.market_store import MarketStore
from data.storage.runtime_store import RuntimeStore
from portfolio.risk_sizer import RiskSizer
from strategy.catalog import (
    MOMENTUM_STRATEGY_ID,
    MOMENTUM_STRATEGY_IDS,
    MOMENTUM_STRATEGY_V2_ID,
    MOMENTUM_STRATEGY_V3_ID,
    TREND_STRATEGY_ID,
    normalize_strategy_id,
    resolve_strategy_config,
)
from strategy.indicators import atr
from strategy.momentum_topn_strategy import MomentumTopNStrategy
from strategy.momentum_topn_v2_strategy import MomentumTopNStrategyV2
from strategy.momentum_topn_v3_strategy import MomentumTopNStrategyV3
from strategy.base import action_value
from strategy.momentum_signal_modules import (
    DEFAULT_MOMENTUM_BUY_FILTERS,
    DEFAULT_MOMENTUM_SELL_SIGNALS,
    normalize_signal_modules,
)
from strategy.trend_score_strategy import TrendScoreStrategy


class BacktestEngine:
    DEFAULT_BENCHMARK_SYMBOL = "512500.SS"

    def __init__(self) -> None:
        self.runtime_store = RuntimeStore()
        self.db = get_db()
        self.market_store = MarketStore()
        self.strategies = {
            TREND_STRATEGY_ID: TrendScoreStrategy(),
            MOMENTUM_STRATEGY_ID: MomentumTopNStrategy(),
            MOMENTUM_STRATEGY_V2_ID: MomentumTopNStrategyV2(),
            MOMENTUM_STRATEGY_V3_ID: MomentumTopNStrategyV3(),
        }

    @staticmethod
    def _load_yaml(path: str) -> dict:
        with Path(path).open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    @staticmethod
    def _to_date(text: str, fallback: date) -> date:
        raw = (text or "").strip()
        if raw == "":
            return fallback
        return datetime.strptime(raw, "%Y-%m-%d").date()

    @staticmethod
    def _prepare_bars(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        bars = df.copy()
        bars["time"] = pd.to_datetime(bars["time"], errors="coerce")
        for col in ("open", "high", "low", "close", "volume", "amount"):
            if col in bars.columns:
                bars[col] = pd.to_numeric(bars[col], errors="coerce")
        bars = bars.dropna(subset=["time", "close", "high", "low"]).sort_values("time").reset_index(drop=True)
        bars["date"] = bars["time"].dt.date
        return bars

    @staticmethod
    def _estimate_buy_cost(qty: int, price: float, fee_rate: float, fee_min: float, slippage: float) -> float:
        if qty <= 0 or price <= 0:
            return 0.0
        deal_price = price * (1.0 + slippage)
        gross = qty * deal_price
        fee = max(gross * fee_rate, fee_min)
        return gross + fee

    @staticmethod
    def _sell_reference_price(signal: dict, position: dict, close_price: float) -> float:
        if close_price <= 0:
            return 0.0
        details = signal.get("calc_details", {})
        stop_triggers: list[str] = []
        if isinstance(details, dict):
            raw = details.get("stop_triggers", [])
            if isinstance(raw, list):
                stop_triggers = [str(item).strip() for item in raw if str(item).strip()]

        reason_parts = [part.strip() for part in str(signal.get("reason", "")).split("|") if part.strip()]
        triggered_tokens = set(stop_triggers + reason_parts)

        hard_stop_triggered = "hard_stop" in triggered_tokens
        if hard_stop_triggered:
            hard_stop_price = float(position.get("hard_stop_price", 0.0) or 0.0)
            if hard_stop_price > 0:
                return hard_stop_price

        chandelier_triggered = "chandelier_stop" in triggered_tokens
        if chandelier_triggered and isinstance(details, dict):
            chandelier_stop_price = float(details.get("chandelier_stop_price", 0.0) or 0.0)
            if chandelier_stop_price > 0:
                return chandelier_stop_price

        ma_breakdown_triggered = any(
            token.startswith("ma") and token.endswith("_breakdown_exit")
            for token in triggered_tokens
        )
        if ma_breakdown_triggered and isinstance(details, dict):
            ma_exit_price = float(details.get("exit_ma_value", 0.0) or 0.0)
            if ma_exit_price > 0:
                return ma_exit_price
        return close_price

    @staticmethod
    def _is_number(value: object) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    @staticmethod
    def _is_true(value: object, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return float(value) != 0.0
        text = str(value or "").strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return default

    @staticmethod
    def _resolve_momentum_buy_filters(strategy_cfg: dict) -> list[str]:
        raw_buy_filters = strategy_cfg.get("buy_filters")
        if isinstance(raw_buy_filters, list) and len(raw_buy_filters) == 0:
            return []
        return normalize_signal_modules(
            raw_buy_filters,
            default=DEFAULT_MOMENTUM_BUY_FILTERS,
        )

    @staticmethod
    def _resolve_momentum_sell_signals(strategy_cfg: dict) -> list[str]:
        raw_sell_signals = strategy_cfg.get("sell_signals")
        if isinstance(raw_sell_signals, list) and len(raw_sell_signals) == 0:
            return []
        return normalize_signal_modules(
            raw_sell_signals,
            default=DEFAULT_MOMENTUM_SELL_SIGNALS,
        )

    def _build_configs(
        self,
        strategy_overrides: dict | None = None,
        instrument_overrides: dict | None = None,
    ) -> tuple[dict, list[dict], dict]:
        strategy_cfg = self._load_yaml("config/strategy.yaml").get("strategy", {})
        instruments_cfg = self._load_yaml("config/instruments.yaml").get("instruments", [])
        app_cfg = self._load_yaml("config/app.yaml").get("app", {})

        for k, v in (strategy_overrides or {}).items():
            if k in strategy_cfg and self._is_number(strategy_cfg.get(k)):
                strategy_cfg[k] = v

        normalized_instruments: list[dict] = []
        for item in instruments_cfg:
            copied = dict(item)
            for k, v in (instrument_overrides or {}).items():
                if k in copied and self._is_number(copied.get(k)):
                    copied[k] = v
            normalized_instruments.append(copied)

        return strategy_cfg, normalized_instruments, app_cfg

    def run(
        self,
        payload: dict,
        strategy_id: str | None = None,
        strategy_overrides: dict | None = None,
        instrument_overrides: dict | None = None,
        persist: bool = True,
        include_charts: bool = True,
        include_trades: bool = True,
    ) -> dict:
        raw_strategy_cfg, instruments_cfg, app_cfg = self._build_configs(
            strategy_overrides=None, instrument_overrides=instrument_overrides
        )
        resolved_strategy_id = normalize_strategy_id(
            strategy_id or payload.get("strategy_id") or raw_strategy_cfg.get("id", TREND_STRATEGY_ID),
            fallback=TREND_STRATEGY_ID,
        )
        strategy_cfg = resolve_strategy_config(
            strategy_cfg=raw_strategy_cfg,
            strategy_id=resolved_strategy_id,
            overrides=strategy_overrides,
        )
        strategy_impl = self.strategies.get(resolved_strategy_id, self.strategies[TREND_STRATEGY_ID])
        is_momentum = resolved_strategy_id in MOMENTUM_STRATEGY_IDS
        momentum_buy_filters = self._resolve_momentum_buy_filters(strategy_cfg) if is_momentum else []
        momentum_sell_signals = self._resolve_momentum_sell_signals(strategy_cfg) if is_momentum else []
        required_history_bars = strategy_impl.required_history_bars(strategy_cfg)

        enabled_instruments = [item for item in instruments_cfg if item.get("enabled", True)]
        selected_symbols_raw = payload.get("selected_symbols", []) or []
        if not isinstance(selected_symbols_raw, (list, tuple, set)):
            selected_symbols_raw = [selected_symbols_raw]
        selected_symbols = {str(s).strip() for s in selected_symbols_raw if str(s).strip()}

        raw_symbol_param_overrides = payload.get("symbol_param_overrides", {}) or {}
        if not isinstance(raw_symbol_param_overrides, dict):
            raw_symbol_param_overrides = {}
        symbol_param_overrides: dict[str, dict[str, float]] = {}
        for raw_symbol, params in raw_symbol_param_overrides.items():
            symbol = str(raw_symbol).strip()
            if symbol == "" or not isinstance(params, dict):
                continue
            normalized: dict[str, float] = {}
            for k in ("hard_stop_atr_mul_default", "chandelier_stop_atr_mul"):
                if k not in params:
                    continue
                try:
                    normalized[k] = float(params[k])
                except (TypeError, ValueError):
                    continue
            if normalized:
                symbol_param_overrides[symbol] = normalized

        def symbol_param(symbol: str, key: str, default: float) -> float:
            value = symbol_param_overrides.get(symbol, {}).get(key, default)
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        if selected_symbols:
            enabled_instruments = [
                item for item in enabled_instruments if str(item.get("symbol", "")).strip() in selected_symbols
            ]
        normalized_enabled_instruments: list[dict] = []
        for item in enabled_instruments:
            symbol = str(item.get("symbol", "")).strip()
            if symbol == "":
                continue
            copied = dict(item)
            copied["symbol"] = symbol
            normalized_enabled_instruments.append(copied)
        enabled_instruments = normalized_enabled_instruments
        symbols = [str(item.get("symbol")) for item in enabled_instruments]

        if not symbols:
            return {
                "run_id": datetime.now().strftime("%Y%m%d%H%M%S"),
                "status": "no_instruments",
                "summary": {},
                "input": {**payload, "strategy_id": resolved_strategy_id},
            }

        default_start = self._to_date(str(strategy_cfg.get("backtest_start_primary", "2025-01-01")), date(2025, 1, 1))
        fallback_start = self._to_date(str(strategy_cfg.get("backtest_start_fallback", "2018-01-01")), date(2018, 1, 1))

        start_date = self._to_date(str(payload.get("start_date", "")), default_start)
        end_date = self._to_date(str(payload.get("end_date", "")), date.today())
        if end_date < start_date:
            start_date, end_date = end_date, start_date

        initial_capital = float(payload.get("initial_capital", 200000.0) or 200000.0)
        lot_size = int(app_cfg.get("lot_size", 100))
        benchmark_mode_raw = str(payload.get("benchmark_mode", "equal_weight_pool")).strip().lower()
        benchmark_mode = benchmark_mode_raw if benchmark_mode_raw in {"equal_weight_pool", "symbol"} else "equal_weight_pool"
        benchmark_symbol = str(payload.get("benchmark_symbol", "")).strip().upper()
        if benchmark_mode == "symbol" and benchmark_symbol == "":
            benchmark_symbol = self.DEFAULT_BENCHMARK_SYMBOL

        market_data: dict[str, pd.DataFrame] = {}
        for symbol in symbols:
            bars = self._prepare_bars(self.market_store.load_history(symbol))
            if bars.empty:
                continue
            market_data[symbol] = bars

        if not market_data:
            run_id = datetime.now().strftime("%Y%m%d%H%M%S")
            result = {
                "run_id": run_id,
                "status": "no_market_data",
                "summary": {},
                "input": {**payload, "strategy_id": resolved_strategy_id},
            }
            if persist:
                self.db.save_backtest(run_id, result)
            return result

        def has_min_data(s: date) -> bool:
            min_need = required_history_bars + 5
            ok_count = 0
            for df in market_data.values():
                rows = df[(df["date"] >= s) & (df["date"] <= end_date)]
                if len(rows) >= min_need:
                    ok_count += 1
            return ok_count >= max(1, len(market_data) // 2)

        start_adjusted = False
        if not has_min_data(start_date) and fallback_start > start_date:
            start_date = fallback_start
            start_adjusted = True

        timeline_set: set[date] = set()
        for df in market_data.values():
            days = df[(df["date"] >= start_date) & (df["date"] <= end_date)]["date"].tolist()
            timeline_set.update(days)
        timeline = sorted(timeline_set)

        run_id = datetime.now().strftime("%Y%m%d%H%M%S")
        if not timeline:
            result = {
                "run_id": run_id,
                "status": "no_data_in_range",
                "summary": {},
                "input": payload,
                "meta": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            }
            if persist:
                self.db.save_backtest(run_id, result)
            return result

        prev_scores: dict[str, float] = {symbol: 0.0 for symbol in symbols}
        prev_prev_scores: dict[str, float] = {symbol: 0.0 for symbol in symbols}
        positions: dict[str, dict] = {
            symbol: {
                "qty": 0,
                "avg_price": 0.0,
                "avg_cost": 0.0,
                "buy_date": None,
                "buy_atr": 0.0,
                "hard_stop_price": 0.0,
                "highest_price": 0.0,
                "sellable_qty": 0,
            }
            for symbol in symbols
        }

        cash = initial_capital
        turnover_total = 0.0
        trades: list[dict] = []
        daily_nav: list[dict] = []
        holdings_history: list[dict] = []
        trend_history: dict[str, list[float | None]] = {symbol: [] for symbol in symbols}
        momentum_rank_history: dict[str, list[float | None]] = {symbol: [] for symbol in symbols}
        hybrid_rank_history: dict[str, list[float | None]] = {symbol: [] for symbol in symbols}
        position_count_history: list[int] = []
        rebalance_history: list[dict] = []
        rebalance_day_count = 0

        fee_rate = float(strategy_cfg.get("fee_rate", 0.000085))
        fee_min = float(strategy_cfg.get("fee_min", 5.0))
        slippage = float(strategy_cfg.get("slippage", 0.002))
        history_tail_bars = max(required_history_bars + 5, 80)
        max_position_vs_equal_weight = float(strategy_cfg.get("max_position_vs_equal_weight", 1.5))
        if max_position_vs_equal_weight <= 0:
            max_position_vs_equal_weight = 1.5
        asset_count = max(len(enabled_instruments), 1)
        max_position_ratio = max_position_vs_equal_weight / asset_count

        risk_sizer = RiskSizer(lot_size=lot_size)

        for day in timeline:
            day_str = day.isoformat()

            signal_map: dict[str, dict] = {}
            day_close_map: dict[str, float] = {}
            day_high_map: dict[str, float] = {}
            for item in enabled_instruments:
                symbol = str(item.get("symbol"))
                df = market_data.get(symbol, pd.DataFrame())
                if df.empty:
                    continue

                day_rows = df[df["date"] == day]
                if day_rows.empty:
                    continue

                day_row = day_rows.iloc[-1]
                day_close = float(day_row["close"])
                day_high = float(day_row["high"])

                day_close_map[symbol] = day_close
                day_high_map[symbol] = day_high

                bars = df[df["date"] <= day].tail(history_tail_bars).copy()

                pos = positions[symbol]
                if pos["qty"] > 0 and pos["buy_date"] is not None:
                    pos["sellable_qty"] = pos["qty"] if str(pos["buy_date"]) < day_str else 0
                    pos["highest_price"] = max(float(pos["highest_price"]), day_high)
                    atr_period = int(strategy_cfg.get("atr_period", 20))
                    atr_series = atr(bars, period=atr_period)
                    current_atr = float(atr_series.iloc[-1]) if not atr_series.empty and pd.notna(atr_series.iloc[-1]) else 0.0

                    chandelier_mul = symbol_param(
                        symbol,
                        "chandelier_stop_atr_mul",
                        float(strategy_cfg.get("chandelier_stop_atr_mul", 2.5)),
                    )
                    chandelier_stop = pos["highest_price"] - chandelier_mul * current_atr if current_atr > 0 else 0.0
                else:
                    pos["sellable_qty"] = 0
                    chandelier_stop = 0.0

                state = {
                    "prev_trend_score": prev_scores.get(symbol, 0.0),
                    "prev_prev_trend_score": prev_prev_scores.get(symbol, 0.0),
                    "position_qty": pos["qty"],
                    "sellable_qty": pos["sellable_qty"],
                    "hard_stop_price": pos["hard_stop_price"],
                    "chandelier_stop_price": chandelier_stop,
                }
                signal = strategy_impl.evaluate(symbol=symbol, bars=bars, state=state, cfg=strategy_cfg)
                signal_map[symbol] = signal

            momentum_plan = {
                "is_rebalance_day": False,
                "planned_holdings": [],
                "to_buy": [],
                "to_sell": [],
            }
            momentum_plan = strategy_impl.finalize_day(
                day=day,
                signal_map=signal_map,
                positions=positions,
                cfg=strategy_cfg,
            )
            if is_momentum and momentum_plan.get("is_rebalance_day"):
                rebalance_day_count += 1
                rebalance_history.append(
                    {
                        "date": day_str,
                        "planned_holdings": momentum_plan.get("planned_holdings", []),
                        "to_buy": momentum_plan.get("to_buy", []),
                        "to_sell": momentum_plan.get("to_sell", []),
                    }
                )

            for symbol in symbols:
                sig = signal_map.get(symbol)
                trend_history[symbol].append(float(sig.get("trend_score", 0.0)) if sig is not None else None)
                if is_momentum:
                    mom_rank = sig.get("momentum_score") if sig is not None else None
                    hybrid_rank = sig.get("hybrid_score") if sig is not None else None
                    momentum_rank_history[symbol].append(float(mom_rank) if self._is_number(mom_rank) else None)
                    hybrid_rank_history[symbol].append(float(hybrid_rank) if self._is_number(hybrid_rank) else None)

            # Execute sells first.
            for item in enabled_instruments:
                symbol = str(item.get("symbol"))
                signal = signal_map.get(symbol)
                if signal is None:
                    continue

                action = action_value(signal.get("action"))
                if action != SignalAction.SELL.value:
                    continue

                pos = positions[symbol]
                sell_qty = int(pos.get("sellable_qty", 0))
                if sell_qty <= 0:
                    continue

                close_price = float(day_close_map.get(symbol, 0.0))
                if close_price <= 0:
                    continue

                sell_ref_price = self._sell_reference_price(
                    signal=signal,
                    position=pos,
                    close_price=close_price,
                )
                if sell_ref_price <= 0:
                    continue
                exec_price = sell_ref_price * (1.0 - slippage)
                gross = sell_qty * exec_price
                fee = max(gross * fee_rate, fee_min)
                net = gross - fee
                cash += net
                turnover_total += gross

                pnl = net - (sell_qty * float(pos.get("avg_cost", pos.get("avg_price", 0.0))))
                trades.append(
                    {
                        "date": day_str,
                        "symbol": symbol,
                        "side": "SELL",
                        "qty": sell_qty,
                        "price": exec_price,
                        "fee": fee,
                        "gross": gross,
                        "net_proceeds": net,
                        "pnl": pnl,
                        "reason": signal.get("reason", ""),
                        "trend_score": float(signal.get("trend_score", 0.0) or 0.0),
                        "momentum_score": float(signal.get("momentum_score", 0.0) or 0.0)
                        if self._is_number(signal.get("momentum_score"))
                        else None,
                        "hybrid_score": float(signal.get("hybrid_score", 0.0) or 0.0)
                        if self._is_number(signal.get("hybrid_score"))
                        else None,
                    }
                )

                positions[symbol] = {
                    "qty": 0,
                    "avg_price": 0.0,
                    "avg_cost": 0.0,
                    "buy_date": None,
                    "buy_atr": 0.0,
                    "hard_stop_price": 0.0,
                    "highest_price": 0.0,
                    "sellable_qty": 0,
                }

            # Build buy candidates and scale to cash.
            market_value_before_buy = 0.0
            for symbol, pos in positions.items():
                qty = int(pos.get("qty", 0))
                if qty <= 0:
                    continue
                close_price = float(day_close_map.get(symbol, pos.get("avg_price", 0.0) or 0.0))
                market_value_before_buy += qty * close_price
            equity_now = cash + market_value_before_buy

            if is_momentum:
                candidates: list[dict] = []
                instrument_map = {str(item.get("symbol")): item for item in enabled_instruments}
                buy_symbols = [
                    symbol
                    for symbol, sig in sorted(
                        signal_map.items(),
                        key=lambda x: float(x[1].get("hybrid_score", -1e9))
                        if self._is_number(x[1].get("hybrid_score"))
                        else -1e9,
                        reverse=True,
                    )
                    if action_value(sig.get("action")) == SignalAction.BUY.value
                ]

                for symbol in buy_symbols:
                    item = instrument_map.get(symbol, {})
                    signal = signal_map.get(symbol)
                    if signal is None:
                        continue
                    if int(positions.get(symbol, {}).get("qty", 0) or 0) > 0:
                        continue

                    atr_value = float(signal.get("calc_details", {}).get("atr", 0.0) or 0.0)
                    close_price = float(day_close_map.get(symbol, 0.0))
                    if atr_value <= 0 or close_price <= 0:
                        continue
                    risk_budget_pct = float(item.get("risk_budget_pct", 0.01))
                    base_stop_mul = float(
                        item.get(
                            "stop_atr_mul",
                            strategy_cfg.get("hard_stop_atr_mul_default", 1.5),
                        )
                    )
                    stop_mul = symbol_param(symbol, "hard_stop_atr_mul_default", base_stop_mul)
                    raw_qty = risk_sizer.suggest_qty(
                        equity=equity_now,
                        risk_budget_pct=risk_budget_pct,
                        atr_value=atr_value,
                        stop_mul=stop_mul,
                    )
                    if raw_qty < lot_size:
                        continue
                    cost = self._estimate_buy_cost(raw_qty, close_price, fee_rate, fee_min, slippage)
                    candidates.append(
                        {
                            "symbol": symbol,
                            "qty": raw_qty,
                            "raw_qty": raw_qty,
                            "cost": cost,
                            "atr": atr_value,
                            "close_price": close_price,
                            "stop_mul": stop_mul,
                        }
                    )

                scaled = risk_sizer.scale_allocations(candidates, total_cash=max(cash, 0.0))
                scaled_map = {str(x.get("symbol")): x for x in scaled}

                for symbol in buy_symbols:
                    s_item = scaled_map.get(symbol)
                    if s_item is None:
                        continue
                    buy_qty = int(s_item.get("scaled_qty", 0))
                    if buy_qty < lot_size:
                        continue
                    close_price = float(s_item.get("close_price", 0.0))
                    exec_price = close_price * (1.0 + slippage)
                    gross = buy_qty * exec_price
                    fee = max(gross * fee_rate, fee_min)
                    total_cost = gross + fee
                    if total_cost > cash:
                        continue

                    cash -= total_cost
                    turnover_total += gross

                    atr_buy = float(s_item.get("atr", 0.0))
                    stop_mul = float(s_item.get("stop_mul", strategy_cfg.get("hard_stop_atr_mul_default", 1.5)))
                    hard_stop_price = exec_price - stop_mul * atr_buy if atr_buy > 0 else 0.0
                    avg_cost = total_cost / buy_qty if buy_qty > 0 else exec_price
                    position_ratio = (total_cost / equity_now) if equity_now > 0 else 0.0

                    positions[symbol] = {
                        "qty": buy_qty,
                        "avg_price": exec_price,
                        "avg_cost": avg_cost,
                        "buy_date": day_str,
                        "buy_atr": atr_buy,
                        "hard_stop_price": hard_stop_price,
                        "highest_price": float(day_high_map.get(symbol, close_price)),
                        "sellable_qty": 0,
                    }

                    trades.append(
                        {
                            "date": day_str,
                            "symbol": symbol,
                            "side": "BUY",
                            "qty": buy_qty,
                            "price": exec_price,
                            "fee": fee,
                            "gross": gross,
                            "total_cost": total_cost,
                            "position_ratio": position_ratio,
                            "reason": signal_map.get(symbol, {}).get("reason", ""),
                            "trend_score": float(signal_map.get(symbol, {}).get("trend_score", 0.0) or 0.0),
                            "momentum_score": float(signal_map.get(symbol, {}).get("momentum_score", 0.0) or 0.0)
                            if self._is_number(signal_map.get(symbol, {}).get("momentum_score"))
                            else None,
                            "hybrid_score": float(signal_map.get(symbol, {}).get("hybrid_score", 0.0) or 0.0)
                            if self._is_number(signal_map.get(symbol, {}).get("hybrid_score"))
                            else None,
                        }
                    )
            else:
                candidates: list[dict] = []
                for item in enabled_instruments:
                    symbol = str(item.get("symbol"))
                    signal = signal_map.get(symbol)
                    if signal is None:
                        continue

                    action = action_value(signal.get("action"))
                    if action != SignalAction.BUY.value:
                        continue
                    if int(positions[symbol].get("qty", 0)) > 0:
                        continue

                    atr_value = float(signal.get("calc_details", {}).get("atr", 0.0) or 0.0)
                    close_price = float(day_close_map.get(symbol, 0.0))
                    if atr_value <= 0 or close_price <= 0:
                        continue

                    risk_budget_pct = float(item.get("risk_budget_pct", 0.01))
                    base_stop_mul = float(item.get("stop_atr_mul", strategy_cfg.get("hard_stop_atr_mul_default", 1.5)))
                    stop_mul = symbol_param(symbol, "hard_stop_atr_mul_default", base_stop_mul)
                    raw_qty = risk_sizer.suggest_qty(
                        equity=equity_now,
                        risk_budget_pct=risk_budget_pct,
                        atr_value=atr_value,
                        stop_mul=stop_mul,
                    )
                    max_position_cost = max(equity_now, 0.0) * max_position_ratio
                    qty = risk_sizer.cap_qty_by_max_cost(
                        qty=raw_qty,
                        price=close_price,
                        max_cost=max_position_cost,
                        fee_rate=fee_rate,
                        fee_min=fee_min,
                        slippage=slippage,
                    )
                    cost = self._estimate_buy_cost(qty, close_price, fee_rate, fee_min, slippage)
                    candidates.append(
                        {
                            "symbol": symbol,
                            "qty": qty,
                            "raw_qty": raw_qty,
                            "cost": cost,
                            "atr": atr_value,
                            "close_price": close_price,
                            "stop_mul": stop_mul,
                            "max_position_ratio": max_position_ratio,
                            "max_position_cost": max_position_cost,
                        }
                    )

                scaled = risk_sizer.scale_allocations(candidates, total_cash=max(cash, 0.0))
                scaled_map = {str(x.get("symbol")): x for x in scaled}

                for item in enabled_instruments:
                    symbol = str(item.get("symbol"))
                    s_item = scaled_map.get(symbol)
                    if s_item is None:
                        continue

                    buy_qty = int(s_item.get("scaled_qty", 0))
                    if buy_qty < lot_size:
                        continue

                    close_price = float(s_item.get("close_price", 0.0))
                    exec_price = close_price * (1.0 + slippage)
                    gross = buy_qty * exec_price
                    fee = max(gross * fee_rate, fee_min)
                    total_cost = gross + fee
                    if total_cost > cash:
                        continue

                    cash -= total_cost
                    turnover_total += gross

                    atr_buy = float(s_item.get("atr", 0.0))
                    stop_mul = float(s_item.get("stop_mul", strategy_cfg.get("hard_stop_atr_mul_default", 1.5)))
                    hard_stop_price = exec_price - stop_mul * atr_buy if atr_buy > 0 else 0.0
                    avg_cost = total_cost / buy_qty if buy_qty > 0 else exec_price
                    position_ratio = (total_cost / equity_now) if equity_now > 0 else 0.0

                    positions[symbol] = {
                        "qty": buy_qty,
                        "avg_price": exec_price,
                        "avg_cost": avg_cost,
                        "buy_date": day_str,
                        "buy_atr": atr_buy,
                        "hard_stop_price": hard_stop_price,
                        "highest_price": float(day_high_map.get(symbol, close_price)),
                        "sellable_qty": 0,
                    }

                    trades.append(
                        {
                            "date": day_str,
                            "symbol": symbol,
                            "side": "BUY",
                            "qty": buy_qty,
                            "price": exec_price,
                            "fee": fee,
                            "gross": gross,
                            "total_cost": total_cost,
                            "position_ratio": position_ratio,
                            "reason": signal_map.get(symbol, {}).get("reason", ""),
                            "trend_score": float(signal_map.get(symbol, {}).get("trend_score", 0.0) or 0.0),
                            "momentum_score": float(signal_map.get(symbol, {}).get("momentum_score", 0.0) or 0.0)
                            if self._is_number(signal_map.get(symbol, {}).get("momentum_score"))
                            else None,
                            "hybrid_score": float(signal_map.get(symbol, {}).get("hybrid_score", 0.0) or 0.0)
                            if self._is_number(signal_map.get(symbol, {}).get("hybrid_score"))
                            else None,
                        }
                    )

            day_symbol_values: dict[str, float] = {}
            market_value = 0.0
            for symbol in symbols:
                pos = positions.get(symbol, {})
                qty = int(pos.get("qty", 0))
                if qty <= 0:
                    day_symbol_values[symbol] = 0.0
                    continue
                close_price = float(day_close_map.get(symbol, pos.get("avg_price", 0.0) or 0.0))
                value = qty * close_price
                day_symbol_values[symbol] = value
                market_value += value

            equity = cash + market_value
            daily_nav.append(
                {
                    "date": day_str,
                    "equity": equity,
                    "cash": cash,
                    "market_value": market_value,
                }
            )
            holdings_history.append(
                {
                    "date": day_str,
                    "cash": cash,
                    "symbols": day_symbol_values,
                }
            )
            position_count_history.append(
                sum(1 for pos in positions.values() if int(pos.get("qty", 0) or 0) > 0)
            )

            for symbol, signal in signal_map.items():
                prev_prev_scores[symbol] = prev_scores.get(symbol, 0.0)
                prev_scores[symbol] = float(signal.get("trend_score", 0.0) or 0.0)

        if benchmark_mode == "symbol":
            if benchmark_symbol == "":
                raise ValueError("benchmark_symbol is required when benchmark_mode=symbol")
            benchmark_data = market_data.get(benchmark_symbol, pd.DataFrame())
            if benchmark_data.empty:
                benchmark_data = self._prepare_bars(self.market_store.load_history(benchmark_symbol))
            if benchmark_data.empty:
                raise ValueError(f"benchmark symbol has no market data: {benchmark_symbol}")

            benchmark_rows = benchmark_data[
                (benchmark_data["date"] >= start_date) & (benchmark_data["date"] <= end_date)
            ]
            if benchmark_rows.empty:
                raise ValueError(
                    f"benchmark symbol has no data in range {start_date.isoformat()} to {end_date.isoformat()}: {benchmark_symbol}"
                )

            benchmark = single_symbol_benchmark(
                benchmark_data=benchmark_data,
                timeline=timeline,
                initial_capital=initial_capital,
                lot_size=lot_size,
                symbol=benchmark_symbol,
            )
        else:
            benchmark = equal_weight_pool_benchmark(
                market_data=market_data,
                timeline=timeline,
                initial_capital=initial_capital,
                lot_size=lot_size,
            )

        drawdown = compute_drawdown(daily_nav)
        summary = compute_metrics(daily_nav=daily_nav, trades=trades, turnover_total=turnover_total)
        benchmark_daily_nav = benchmark.get("series", [])
        benchmark_summary = compute_metrics(daily_nav=benchmark_daily_nav, trades=[], turnover_total=0.0)
        annual_returns = compute_annual_returns(
            daily_nav=daily_nav,
            trades=trades,
            benchmark_daily_nav=benchmark_daily_nav,
        )
        monthly_heatmap = compute_monthly_heatmap(daily_nav)
        symbol_stats = compute_symbol_trade_stats(trades, symbols=symbols)
        strategy_highlights: dict = {}
        if is_momentum:
            rebalance_buy_count = sum(len(item.get("to_buy", [])) for item in rebalance_history)
            rebalance_sell_count = sum(len(item.get("to_sell", [])) for item in rebalance_history)
            avg_position_count = (
                float(sum(position_count_history)) / len(position_count_history)
                if position_count_history
                else 0.0
            )
            strategy_highlights = {
                "max_holdings": int(strategy_cfg.get("max_holdings", 5)),
                "rebalance_day_count": int(rebalance_day_count),
                "rebalance_buy_count": int(rebalance_buy_count),
                "rebalance_sell_count": int(rebalance_sell_count),
                "avg_position_count": float(avg_position_count),
                "buy_filters": list(momentum_buy_filters),
                "sell_signals": list(momentum_sell_signals),
                "required_history_bars": int(required_history_bars),
            }

        nav_dates = [item["date"] for item in daily_nav]
        nav_values = [float(item["equity"]) for item in daily_nav]

        bm_map = {item["date"]: float(item["equity"]) for item in benchmark.get("series", [])}
        benchmark_values = [bm_map.get(d, None) for d in nav_dates]

        nav_by_date = {item["date"]: float(item["equity"]) for item in daily_nav}
        buy_points = []
        sell_points = []
        for tr in trades:
            point = {
                "date": tr.get("date"),
                "value": nav_by_date.get(str(tr.get("date")), None),
                "symbol": tr.get("symbol"),
                "qty": tr.get("qty"),
                "price": tr.get("price"),
                "amount": tr.get("total_cost") if tr.get("side") == "BUY" else tr.get("net_proceeds"),
                "trend_score": tr.get("trend_score"),
                "momentum_score": tr.get("momentum_score"),
                "hybrid_score": tr.get("hybrid_score"),
            }
            if tr.get("side") == "BUY":
                buy_points.append(point)
            elif tr.get("side") == "SELL":
                sell_points.append(point)

        trend_series = {symbol: trend_history.get(symbol, []) for symbol in symbols}
        holdings_series = {
            "dates": [row.get("date") for row in holdings_history],
            "order": [*symbols, "CASH"],
            "series": {
                **{symbol: [float((row.get("symbols") or {}).get(symbol, 0.0) or 0.0) for row in holdings_history] for symbol in symbols},
                "CASH": [float(row.get("cash", 0.0) or 0.0) for row in holdings_history],
            },
        }

        kline_ma_periods = [20, 30, 40, 60, 200]
        kline_data: dict[str, dict] = {}
        for symbol in symbols:
            df = market_data.get(symbol, pd.DataFrame())
            if df.empty:
                continue
            full_rows = df[df["date"] <= end_date].copy()
            if full_rows.empty:
                continue
            rows = full_rows[full_rows["date"] >= start_date].copy()
            if rows.empty:
                continue

            close_full = pd.to_numeric(full_rows.get("close"), errors="coerce")
            ma_series_map: dict[str, list[float | None]] = {}
            for period in kline_ma_periods:
                ma_full = close_full.rolling(period, min_periods=period).mean()
                ma_aligned = ma_full.loc[rows.index]
                ma_series_map[str(period)] = [
                    (float(v) if pd.notna(v) else None) for v in ma_aligned.tolist()
                ]

            kline_data[symbol] = {
                "dates": [d.isoformat() for d in rows["date"].tolist()],
                "candles": [
                    [
                        float(r["open"]),
                        float(r["close"]),
                        float(r["low"]),
                        float(r["high"]),
                    ]
                    for _, r in rows.iterrows()
                ],
                "ma": ma_series_map,
                "buy_points": [],
                "sell_points": [],
            }

        for tr in trades:
            symbol = str(tr.get("symbol", "")).strip()
            if symbol == "" or symbol not in kline_data:
                continue

            p = {
                "date": str(tr.get("date", "")),
                "price": float(tr.get("price", 0.0) or 0.0),
                "amount": tr.get("total_cost") if str(tr.get("side", "")).upper() == "BUY" else tr.get("net_proceeds"),
                "qty": tr.get("qty"),
                "trend_score": tr.get("trend_score"),
                "momentum_score": tr.get("momentum_score"),
                "hybrid_score": tr.get("hybrid_score"),
            }
            if str(tr.get("side", "")).upper() == "BUY":
                kline_data[symbol]["buy_points"].append(p)
            elif str(tr.get("side", "")).upper() == "SELL":
                kline_data[symbol]["sell_points"].append(p)

        result = {
            "run_id": run_id,
            "status": "ok",
            "input": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "initial_capital": initial_capital,
                "selected_symbols": symbols,
                "strategy_id": resolved_strategy_id,
                "benchmark_mode": benchmark_mode,
                "benchmark_symbol": benchmark_symbol,
                "strategy_overrides": (strategy_overrides or {}),
                "symbol_param_overrides": symbol_param_overrides,
            },
            "meta": {
                "strategy_id": resolved_strategy_id,
                "symbols": symbols,
                "start_adjusted_to_fallback": start_adjusted,
                "timeline_days": len(timeline),
                "benchmark": benchmark.get("name", "equal_weight_pool"),
            },
            "summary": summary,
            "strategy_highlights": strategy_highlights,
            "benchmark_summary": benchmark_summary,
            "annual_returns": annual_returns,
            "monthly_heatmap": monthly_heatmap,
            "symbol_stats": symbol_stats,
        }

        if include_trades:
            result["trades"] = trades
        else:
            result["trades"] = []

        if include_charts:
            charts_payload = {
                "dates": nav_dates,
                "nav": nav_values,
                "benchmark_nav": benchmark_values,
                "drawdown": drawdown,
                "buy_points": buy_points,
                "sell_points": sell_points,
                "trend": {
                    "dates": nav_dates,
                    "series": trend_series,
                },
                "holdings": holdings_series,
                "kline": kline_data,
            }
            if is_momentum:
                charts_payload["ranking"] = {
                    "dates": nav_dates,
                    "momentum": momentum_rank_history,
                    "hybrid": hybrid_rank_history,
                    "position_count": position_count_history,
                    "rebalance_events": rebalance_history,
                }
            result["charts"] = charts_payload
        else:
            result["charts"] = {}

        if persist:
            self.db.save_backtest(run_id, result)
        return result



















