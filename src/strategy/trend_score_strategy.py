from __future__ import annotations

import pandas as pd

from core.enums import SignalAction, SignalLevel
from strategy.base import BaseStrategy, EntryDecision
from strategy.features import build_trend_score_features
from strategy.trend_score_core import safe_float


class TrendScoreStrategy(BaseStrategy):
    name = "trend_score_v1"

    @staticmethod
    def _entry_threshold_range(cfg: dict) -> tuple[float, float]:
        lower = safe_float(cfg.get("entry_threshold_min", cfg.get("entry_threshold", 10.0)), 10.0)
        upper = safe_float(cfg.get("entry_threshold_max", 20.0), 20.0)
        if upper < lower:
            lower, upper = upper, lower
        return lower, upper

    def compute_features(self, symbol: str, bars: pd.DataFrame, state: dict, cfg: dict) -> dict:
        _ = symbol
        features = build_trend_score_features(bars=bars, state=state, cfg=cfg)
        calc_details = features.get("calc_details", {})
        if isinstance(calc_details, dict):
            lower, upper = self._entry_threshold_range(cfg)
            calc_details["entry_threshold_min"] = lower
            calc_details["entry_threshold_max"] = upper
            features["calc_details"] = calc_details
        return features

    def evaluate_entry_signal(self, signal: dict, state: dict, cfg: dict) -> EntryDecision:
        position_qty = int(state.get("position_qty", 0) or 0)
        if position_qty > 0:
            return EntryDecision(triggered=False, reason="")

        details = signal.get("calc_details", {})
        if not isinstance(details, dict):
            details = {}

        current_price = safe_float(details.get("price", 0.0), 0.0)
        ma_mid = safe_float(details.get("ma_mid", 0.0), 0.0)
        trend_score = safe_float(signal.get("trend_score", 0.0), 0.0)
        entry_threshold_min, entry_threshold_max = self._entry_threshold_range(cfg)

        is_entry_window = (
            trend_score >= entry_threshold_min
            and trend_score <= entry_threshold_max
            and current_price > ma_mid
        )
        return EntryDecision(triggered=is_entry_window, reason="entry_window")

    def default_hold_reason(self, signal: dict, state: dict, cfg: dict) -> str:
        _ = state
        _ = cfg
        if not bool(signal.get("ok", False)):
            return str(signal.get("reason", "invalid_trend_snapshot"))
        return "no_trigger"

    def decorate_signal(self, signal: dict, state: dict, cfg: dict) -> dict:
        if not bool(signal.get("ok", False)):
            signal["level"] = SignalLevel.WARN
            signal["reason"] = str(signal.get("reason", "invalid_trend_snapshot"))
            return signal

        if signal.get("action") == SignalAction.SELL or str(signal.get("reason", "")).startswith("t1_blocked:"):
            return signal

        details = signal.get("calc_details", {})
        if not isinstance(details, dict):
            details = {}

        trend_score = safe_float(signal.get("trend_score", 0.0), 0.0)
        current_price = safe_float(details.get("price", 0.0), 0.0)
        ma_mid = safe_float(details.get("ma_mid", 0.0), 0.0)
        entry_threshold_min, entry_threshold_max = self._entry_threshold_range(cfg)
        position_qty = int(state.get("position_qty", 0) or 0)

        if signal.get("action") == SignalAction.BUY:
            signal["level"] = SignalLevel.ACTION
            return signal

        if position_qty <= 0:
            if trend_score >= (entry_threshold_min * 0.8) and trend_score < entry_threshold_min and current_price > ma_mid:
                signal["level"] = SignalLevel.WARN
                signal["reason"] = "entry_watch"
            elif trend_score > entry_threshold_max and current_price > ma_mid:
                signal["level"] = SignalLevel.WARN
                signal["reason"] = "entry_overheat"
        return signal
