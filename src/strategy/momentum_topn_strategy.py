from __future__ import annotations

import pandas as pd

from strategy.base import BaseStrategy, EntryDecision
from strategy.entry_filters import build_entry_filters
from strategy.features import build_momentum_features
from strategy.momentum_signal_modules import DEFAULT_MOMENTUM_BUY_FILTERS, normalize_signal_modules
from strategy.planners import WeeklyTopNRebalancePlanner


class MomentumTopNStrategy(BaseStrategy):
    name = "momentum_topn_v1"

    def compute_features(self, symbol: str, bars: pd.DataFrame, state: dict, cfg: dict) -> dict:
        _ = symbol
        return build_momentum_features(bars=bars, state=state, cfg=cfg)

    def get_entry_filters(self, cfg: dict) -> list[object]:
        raw_buy_filters = cfg.get("buy_filters")
        if isinstance(raw_buy_filters, list) and len(raw_buy_filters) == 0:
            buy_filters: list[str] = []
        else:
            buy_filters = normalize_signal_modules(raw_buy_filters, default=DEFAULT_MOMENTUM_BUY_FILTERS)
        return build_entry_filters(buy_filters)

    def evaluate_entry_signal(self, signal: dict, state: dict, cfg: dict) -> EntryDecision:
        _ = state
        _ = cfg
        return EntryDecision(triggered=bool(signal.get("ok", False)), reason="rebalance_candidate")

    def get_cross_section_planner(self, cfg: dict) -> object:
        _ = cfg
        return WeeklyTopNRebalancePlanner()

    def required_history_bars(self, cfg: dict) -> int:
        required = [
            int(cfg.get("lookback_days", 120)),
            int(cfg.get("n_long", 40)),
            int(cfg.get("atr_period", 20)),
            int(cfg.get("momentum_window_short", 10)),
            int(cfg.get("momentum_window_long", 20)),
        ]
        raw_buy_filters = cfg.get("buy_filters")
        if not (isinstance(raw_buy_filters, list) and len(raw_buy_filters) == 0):
            buy_filters = normalize_signal_modules(raw_buy_filters, default=DEFAULT_MOMENTUM_BUY_FILTERS)
            if "price_above_ma20" in buy_filters:
                required.append(20)
            if "price_above_ma60" in buy_filters:
                required.append(60)
            if "price_above_ma200" in buy_filters:
                required.append(200)
        return max(required) if required else 120
