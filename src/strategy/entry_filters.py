from __future__ import annotations

from dataclasses import dataclass

from strategy.base import FilterDecision
from strategy.momentum_signal_modules import (
    BUY_FILTER_PRICE_ABOVE_MA20,
    BUY_FILTER_PRICE_ABOVE_MA60,
    BUY_FILTER_PRICE_ABOVE_MA200,
    BUY_FILTER_TREND_SCORE_MAX,
)
from strategy.trend_score_core import safe_float


def _details(signal: dict) -> dict:
    details = signal.get("calc_details", {})
    if isinstance(details, dict):
        return details
    return {}


@dataclass(slots=True)
class PriceAboveMa20Filter:
    filter_id: str = BUY_FILTER_PRICE_ABOVE_MA20

    def evaluate(self, signal: dict, cfg: dict) -> FilterDecision:
        _ = cfg
        details = _details(signal)
        price = safe_float(details.get("price", 0.0), 0.0)
        ma20 = safe_float(details.get("ma20", details.get("ma_mid", 0.0)), 0.0)
        return FilterDecision(
            passed=(price > 0) and (ma20 > 0) and (price > ma20),
            reason=self.filter_id,
            filter_id=self.filter_id,
        )


@dataclass(slots=True)
class PriceAboveMa60Filter:
    filter_id: str = BUY_FILTER_PRICE_ABOVE_MA60

    def evaluate(self, signal: dict, cfg: dict) -> FilterDecision:
        _ = cfg
        details = _details(signal)
        price = safe_float(details.get("price", 0.0), 0.0)
        ma60 = safe_float(details.get("ma60", 0.0), 0.0)
        return FilterDecision(
            passed=(price > 0) and (ma60 > 0) and (price > ma60),
            reason=self.filter_id,
            filter_id=self.filter_id,
        )


@dataclass(slots=True)
class PriceAboveMa200Filter:
    filter_id: str = BUY_FILTER_PRICE_ABOVE_MA200

    def evaluate(self, signal: dict, cfg: dict) -> FilterDecision:
        _ = cfg
        details = _details(signal)
        price = safe_float(details.get("price", 0.0), 0.0)
        ma200 = safe_float(details.get("ma200", 0.0), 0.0)
        return FilterDecision(
            passed=(price > 0) and (ma200 > 0) and (price > ma200),
            reason=self.filter_id,
            filter_id=self.filter_id,
        )


@dataclass(slots=True)
class TrendScoreMaxFilter:
    filter_id: str = BUY_FILTER_TREND_SCORE_MAX

    def evaluate(self, signal: dict, cfg: dict) -> FilterDecision:
        max_entry_trend_score = safe_float(cfg.get("max_entry_trend_score", 0.0), 0.0)
        trend_score = signal.get("trend_score")
        passed = False
        if max_entry_trend_score > 0 and isinstance(trend_score, (int, float)) and not isinstance(trend_score, bool):
            passed = float(trend_score) <= max_entry_trend_score
        return FilterDecision(
            passed=passed,
            reason=self.filter_id,
            filter_id=self.filter_id,
            meta={"max_entry_trend_score": max_entry_trend_score},
        )


@dataclass(slots=True)
class UnknownEntryFilter:
    filter_id: str

    def evaluate(self, signal: dict, cfg: dict) -> FilterDecision:
        _ = signal
        _ = cfg
        return FilterDecision(passed=False, reason=self.filter_id, filter_id=self.filter_id)


def build_entry_filters(filter_ids: list[str]) -> list[object]:
    registry = {
        BUY_FILTER_PRICE_ABOVE_MA20: PriceAboveMa20Filter,
        BUY_FILTER_PRICE_ABOVE_MA60: PriceAboveMa60Filter,
        BUY_FILTER_PRICE_ABOVE_MA200: PriceAboveMa200Filter,
        BUY_FILTER_TREND_SCORE_MAX: TrendScoreMaxFilter,
    }
    built: list[object] = []
    for filter_id in filter_ids:
        cls = registry.get(filter_id)
        if cls is None:
            built.append(UnknownEntryFilter(filter_id=filter_id))
        else:
            built.append(cls())
    return built
