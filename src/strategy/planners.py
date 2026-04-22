from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np

from core.enums import SignalAction, SignalLevel
from strategy.base import ExitDecision, action_value, apply_exit_decisions


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _zscore_map(values: dict[str, float | None]) -> dict[str, float]:
    valid_items = [(key, float(value)) for key, value in values.items() if value is not None]
    if len(valid_items) <= 1:
        return {key: 0.0 for key, _ in valid_items}
    arr = np.array([value for _, value in valid_items], dtype=float)
    mean = float(arr.mean())
    std = float(arr.std(ddof=0))
    if std <= 1e-12:
        return {key: 0.0 for key, _ in valid_items}
    return {key: (value - mean) / std for key, value in valid_items}


@dataclass(slots=True)
class WeeklyTopNRebalancePlanner:
    planner_id: str = "weekly_topn_rebalance"

    @staticmethod
    def _is_rebalance_day(day: date, cfg: dict) -> bool:
        frequency = str(cfg.get("rebalance_frequency", "weekly")).strip().lower()
        if frequency != "weekly":
            return False
        weekday = int(cfg.get("rebalance_weekday", 1))
        return int(day.isoweekday()) == weekday

    def _apply_rank_scores(self, signal_map: dict[str, dict], cfg: dict) -> None:
        mom_short: dict[str, float | None] = {}
        mom_long: dict[str, float | None] = {}
        trend_score: dict[str, float | None] = {}

        for symbol, signal in signal_map.items():
            short_v = signal.get("momentum_short")
            long_v = signal.get("momentum_long")
            trend_v = signal.get("trend_score")
            mom_short[symbol] = float(short_v) if _is_number(short_v) else None
            mom_long[symbol] = float(long_v) if _is_number(long_v) else None
            trend_score[symbol] = float(trend_v) if _is_number(trend_v) else None

        short_z = _zscore_map(mom_short)
        long_z = _zscore_map(mom_long)
        trend_z = _zscore_map(trend_score)

        w_short = float(cfg.get("momentum_weight_short", 0.6))
        w_long = float(cfg.get("momentum_weight_long", 0.4))
        w_momentum = float(cfg.get("hybrid_weight_momentum", 1.0))
        w_trend = float(cfg.get("hybrid_weight_trend", 0.0))

        for symbol, signal in signal_map.items():
            has_momentum = (symbol in short_z) and (symbol in long_z)
            momentum_score = (
                w_short * short_z.get(symbol, 0.0) + w_long * long_z.get(symbol, 0.0)
            ) if has_momentum else None
            hybrid_score = (
                w_momentum * momentum_score + w_trend * trend_z.get(symbol, 0.0)
            ) if momentum_score is not None else None
            signal["momentum_score"] = momentum_score
            signal["hybrid_score"] = hybrid_score

            details = signal.get("calc_details", {})
            if not isinstance(details, dict):
                details = {}
            details["momentum_rank_z"] = momentum_score
            details["trend_rank_z"] = trend_z.get(symbol)
            details["hybrid_score"] = hybrid_score
            signal["calc_details"] = details

    def plan(
        self,
        day: date,
        signal_map: dict[str, dict],
        positions: dict[str, dict],
        cfg: dict,
    ) -> dict:
        self._apply_rank_scores(signal_map=signal_map, cfg=cfg)

        if not self._is_rebalance_day(day=day, cfg=cfg):
            return {
                "is_rebalance_day": False,
                "planned_holdings": [],
                "to_buy": [],
                "to_sell": [],
            }

        max_holdings = max(int(cfg.get("max_holdings", 5)), 1)
        current_holdings = {
            symbol
            for symbol, position in positions.items()
            if int(position.get("qty", 0) or 0) > 0
            and action_value(signal_map.get(symbol, {}).get("action")) != SignalAction.SELL.value
        }

        ranked_all: list[tuple[str, float]] = []
        for symbol, signal in signal_map.items():
            hybrid_score = signal.get("hybrid_score")
            if not _is_number(hybrid_score):
                continue
            ranked_all.append((symbol, float(hybrid_score)))
        ranked_all.sort(key=lambda item: item[1], reverse=True)

        top_keep_count = (len(ranked_all) + 1) // 2
        ranked_top_half = ranked_all[:top_keep_count]
        top_half_set = {symbol for symbol, _ in ranked_top_half}
        ranked_candidates = [
            (symbol, score)
            for symbol, score in ranked_top_half
            if bool(signal_map.get(symbol, {}).get("entry_passed", False))
        ]

        ranking_index = {symbol: idx for idx, (symbol, _score) in enumerate(ranked_all)}
        kept_holdings = {symbol for symbol in current_holdings if symbol in top_half_set}
        to_sell: list[str] = []
        blocked_sell: list[str] = []

        for symbol in sorted(current_holdings - top_half_set):
            signal = signal_map.get(symbol)
            if signal is None:
                continue
            state = {
                "sellable_qty": int(positions.get(symbol, {}).get("sellable_qty", 0) or 0),
            }
            decision = ExitDecision(
                triggered=True,
                reason="rebalance_outside_top50",
                scope="strategy",
                meta={},
            )
            sold = apply_exit_decisions(signal=signal, state=state, decisions=[decision])
            if sold and action_value(signal.get("action")) == SignalAction.SELL.value:
                to_sell.append(symbol)
            else:
                blocked_sell.append(symbol)
                kept_holdings.add(symbol)

        remaining_slots = max(max_holdings - len(kept_holdings), 0)
        to_buy: list[str] = []
        for symbol, _score in ranked_candidates:
            if remaining_slots <= 0:
                break
            if symbol in kept_holdings:
                continue
            signal = signal_map.get(symbol)
            if signal is None:
                continue
            if action_value(signal.get("action")) == SignalAction.SELL.value:
                continue
            signal["action"] = SignalAction.BUY
            signal["level"] = SignalLevel.ACTION
            signal["reason"] = "rebalance_entry"
            to_buy.append(symbol)
            kept_holdings.add(symbol)
            remaining_slots -= 1

        planned_holdings = sorted(
            kept_holdings,
            key=lambda symbol: ranking_index.get(symbol, len(ranking_index)),
        )
        return {
            "is_rebalance_day": True,
            "planned_holdings": planned_holdings,
            "to_buy": to_buy,
            "to_sell": to_sell,
            "blocked_sell": blocked_sell,
        }
