from __future__ import annotations

from copy import deepcopy

from strategy.momentum_signal_modules import (
    BUY_FILTER_PRICE_ABOVE_MA20,
    BUY_FILTER_PRICE_ABOVE_MA60,
    BUY_FILTER_TREND_SCORE_MAX,
    DEFAULT_MOMENTUM_BUY_FILTERS,
    DEFAULT_MOMENTUM_SELL_SIGNALS,
    SELL_SIGNAL_MA_BREAKDOWN_MAX,
    normalize_signal_modules,
)


TREND_STRATEGY_ID = "trend_score_v1"
MOMENTUM_STRATEGY_ID = "momentum_topn_v1"
MOMENTUM_STRATEGY_V2_ID = "momentum_topn_v2"
MOMENTUM_STRATEGY_V3_ID = "momentum_topn_v3"
MOMENTUM_STRATEGY_IDS = {
    MOMENTUM_STRATEGY_ID,
    MOMENTUM_STRATEGY_V2_ID,
    MOMENTUM_STRATEGY_V3_ID,
}

MOMENTUM_V1_DEFAULTS = {
    "max_holdings": 5,
    "ranking_mode": "hybrid",
    "momentum_window_short": 10,
    "momentum_window_long": 20,
    "momentum_weight_short": 0.6,
    "momentum_weight_long": 0.4,
    "hybrid_weight_momentum": 1.0,
    "hybrid_weight_trend": 0.0,
    "rebalance_frequency": "weekly",
    "rebalance_weekday": 1,
    "buy_filters": list(DEFAULT_MOMENTUM_BUY_FILTERS),
    "sell_signals": list(DEFAULT_MOMENTUM_SELL_SIGNALS),
}

MOMENTUM_V2_DEFAULTS = {
    **MOMENTUM_V1_DEFAULTS,
    "buy_filter_price_above_ma60": True,
    "exit_ma_period": 30,
    "buy_filters": [BUY_FILTER_PRICE_ABOVE_MA20, BUY_FILTER_PRICE_ABOVE_MA60],
    "sell_signals": [*DEFAULT_MOMENTUM_SELL_SIGNALS, SELL_SIGNAL_MA_BREAKDOWN_MAX],
}

MOMENTUM_V3_DEFAULTS = {
    **MOMENTUM_V2_DEFAULTS,
    "max_entry_trend_score": 20.0,
    "buy_filters": [BUY_FILTER_PRICE_ABOVE_MA20, BUY_FILTER_PRICE_ABOVE_MA60, BUY_FILTER_TREND_SCORE_MAX],
    "sell_signals": [*DEFAULT_MOMENTUM_SELL_SIGNALS, SELL_SIGNAL_MA_BREAKDOWN_MAX],
}


def _trend_entry_range(cfg: dict) -> tuple[float, float]:
    lower = float(cfg.get("entry_threshold_min", cfg.get("entry_threshold", 10.0)))
    upper = float(cfg.get("entry_threshold_max", 20.0))
    if upper < lower:
        lower, upper = upper, lower
    return lower, upper


def normalize_strategy_id(raw_id: object, fallback: str = TREND_STRATEGY_ID) -> str:
    text = str(raw_id or "").strip()
    if text in {TREND_STRATEGY_ID, MOMENTUM_STRATEGY_ID, MOMENTUM_STRATEGY_V2_ID, MOMENTUM_STRATEGY_V3_ID}:
        return text
    return fallback


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _apply_overrides(base_cfg: dict, overrides: dict | None) -> dict:
    merged = dict(base_cfg)
    for k, v in (overrides or {}).items():
        if k not in merged:
            continue
        if _is_number(merged.get(k)):
            merged[k] = v
            continue
        if isinstance(merged.get(k), str):
            merged[k] = str(v)
            continue
        if isinstance(merged.get(k), bool):
            merged[k] = bool(v)
            continue
        if isinstance(merged.get(k), list):
            if isinstance(v, list) and len(v) == 0:
                merged[k] = []
                continue
            merged[k] = normalize_signal_modules(v, default=list(merged.get(k) or []))
    return merged


def _normalize_momentum_signal_config(cfg: dict, strategy_id: str) -> dict:
    merged = dict(cfg)

    if strategy_id == MOMENTUM_STRATEGY_V3_ID:
        default_buy_filters = [BUY_FILTER_PRICE_ABOVE_MA20, BUY_FILTER_PRICE_ABOVE_MA60, BUY_FILTER_TREND_SCORE_MAX]
        default_sell_signals = [*DEFAULT_MOMENTUM_SELL_SIGNALS, SELL_SIGNAL_MA_BREAKDOWN_MAX]
    elif strategy_id == MOMENTUM_STRATEGY_V2_ID:
        default_buy_filters = [BUY_FILTER_PRICE_ABOVE_MA20, BUY_FILTER_PRICE_ABOVE_MA60]
        default_sell_signals = [*DEFAULT_MOMENTUM_SELL_SIGNALS, SELL_SIGNAL_MA_BREAKDOWN_MAX]
    else:
        default_buy_filters = list(DEFAULT_MOMENTUM_BUY_FILTERS)
        default_sell_signals = list(DEFAULT_MOMENTUM_SELL_SIGNALS)

    raw_buy_filters = merged.get("buy_filters")
    raw_sell_signals = merged.get("sell_signals")
    if isinstance(raw_buy_filters, list) and len(raw_buy_filters) == 0:
        buy_filters = []
    else:
        buy_filters = normalize_signal_modules(raw_buy_filters, default=default_buy_filters)
    if isinstance(raw_sell_signals, list) and len(raw_sell_signals) == 0:
        sell_signals = []
    else:
        sell_signals = normalize_signal_modules(raw_sell_signals, default=default_sell_signals)

    # Backward-compatible flag mapping for old configs.
    if not bool(merged.get("buy_filter_price_above_ma60", True)):
        buy_filters = [x for x in buy_filters if x != BUY_FILTER_PRICE_ABOVE_MA60]

    merged["buy_filters"] = buy_filters
    merged["sell_signals"] = sell_signals
    return merged


def resolve_strategy_config(strategy_cfg: dict, strategy_id: str, overrides: dict | None = None) -> dict:
    sid = normalize_strategy_id(strategy_id, fallback=str(strategy_cfg.get("id", TREND_STRATEGY_ID)))
    shared = dict(strategy_cfg or {})
    momentum_v1_cfg = shared.pop("momentum_topn", {})
    momentum_v2_cfg = shared.pop("momentum_topn_v2", {})
    momentum_v3_cfg = shared.pop("momentum_topn_v3", {})
    if not isinstance(momentum_v1_cfg, dict):
        momentum_v1_cfg = {}
    if not isinstance(momentum_v2_cfg, dict):
        momentum_v2_cfg = {}
    if not isinstance(momentum_v3_cfg, dict):
        momentum_v3_cfg = {}

    if sid == MOMENTUM_STRATEGY_ID:
        merged = {**shared, **MOMENTUM_V1_DEFAULTS, **momentum_v1_cfg}
        merged["id"] = sid
        merged = _apply_overrides(merged, overrides)
        return _normalize_momentum_signal_config(merged, sid)

    if sid == MOMENTUM_STRATEGY_V2_ID:
        merged = {**shared, **MOMENTUM_V2_DEFAULTS, **momentum_v1_cfg, **momentum_v2_cfg}
        merged["id"] = sid
        merged = _apply_overrides(merged, overrides)
        return _normalize_momentum_signal_config(merged, sid)

    if sid == MOMENTUM_STRATEGY_V3_ID:
        merged = {
            **shared,
            **MOMENTUM_V3_DEFAULTS,
            **momentum_v1_cfg,
            **momentum_v2_cfg,
            **momentum_v3_cfg,
        }
        merged["id"] = sid
        merged = _apply_overrides(merged, overrides)
        return _normalize_momentum_signal_config(merged, sid)

    merged = dict(shared)
    merged["id"] = TREND_STRATEGY_ID
    return _apply_overrides(merged, overrides)


def build_strategy_catalog(strategy_cfg: dict) -> dict:
    strategy_cfg = strategy_cfg if isinstance(strategy_cfg, dict) else {}
    default_id = normalize_strategy_id(
        strategy_cfg.get("id", TREND_STRATEGY_ID), fallback=TREND_STRATEGY_ID
    )
    trend_cfg = resolve_strategy_config(strategy_cfg, TREND_STRATEGY_ID)
    trend_entry_min, trend_entry_max = _trend_entry_range(trend_cfg)
    momentum_v1_cfg = resolve_strategy_config(strategy_cfg, MOMENTUM_STRATEGY_ID)
    momentum_v2_cfg = resolve_strategy_config(strategy_cfg, MOMENTUM_STRATEGY_V2_ID)
    momentum_v3_cfg = resolve_strategy_config(strategy_cfg, MOMENTUM_STRATEGY_V3_ID)

    return {
        "default_id": default_id,
        "items": [
            {
                "id": TREND_STRATEGY_ID,
                "name": "Trend Score v1",
                "summary": "Single-asset trend score with stop rules",
                "params": {
                    "n_short": int(trend_cfg.get("n_short", 5)),
                    "n_mid": int(trend_cfg.get("n_mid", 20)),
                    "n_long": int(trend_cfg.get("n_long", 40)),
                    "entry_threshold_min": float(trend_entry_min),
                    "entry_threshold_max": float(trend_entry_max),
                },
            },
            {
                "id": MOMENTUM_STRATEGY_V3_ID,
                "name": "Momentum TopN v3",
                "summary": "v2 plus buy-day trend_score cap (<=20)",
                "params": {
                    "n_short": int(momentum_v3_cfg.get("n_short", 5)),
                    "n_mid": int(momentum_v3_cfg.get("n_mid", 20)),
                    "n_long": int(momentum_v3_cfg.get("n_long", 40)),
                    "entry_threshold": float(momentum_v3_cfg.get("entry_threshold", 10.0)),
                    "max_holdings": int(momentum_v3_cfg.get("max_holdings", 5)),
                    "momentum_window_short": int(momentum_v3_cfg.get("momentum_window_short", 10)),
                    "momentum_window_long": int(momentum_v3_cfg.get("momentum_window_long", 20)),
                    "momentum_weight_short": float(momentum_v3_cfg.get("momentum_weight_short", 0.6)),
                    "momentum_weight_long": float(momentum_v3_cfg.get("momentum_weight_long", 0.4)),
                    "hybrid_weight_momentum": float(momentum_v3_cfg.get("hybrid_weight_momentum", 1.0)),
                    "hybrid_weight_trend": float(momentum_v3_cfg.get("hybrid_weight_trend", 0.0)),
                    "rebalance_weekday": int(momentum_v3_cfg.get("rebalance_weekday", 1)),
                    "buy_filters": list(momentum_v3_cfg.get("buy_filters", MOMENTUM_V3_DEFAULTS["buy_filters"])),
                    "sell_signals": list(momentum_v3_cfg.get("sell_signals", MOMENTUM_V3_DEFAULTS["sell_signals"])),
                    "exit_ma_period": int(momentum_v3_cfg.get("exit_ma_period", 30)),
                    "max_entry_trend_score": float(momentum_v3_cfg.get("max_entry_trend_score", 20.0)),
                },
            },
            {
                "id": MOMENTUM_STRATEGY_V2_ID,
                "name": "Momentum TopN v2",
                "summary": "TopN momentum with MA20+MA60 buy filters and max(MA30,MA40,MA60) exit",
                "params": {
                    "n_short": int(momentum_v2_cfg.get("n_short", 5)),
                    "n_mid": int(momentum_v2_cfg.get("n_mid", 20)),
                    "n_long": int(momentum_v2_cfg.get("n_long", 40)),
                    "entry_threshold": float(momentum_v2_cfg.get("entry_threshold", 10.0)),
                    "max_holdings": int(momentum_v2_cfg.get("max_holdings", 5)),
                    "momentum_window_short": int(momentum_v2_cfg.get("momentum_window_short", 10)),
                    "momentum_window_long": int(momentum_v2_cfg.get("momentum_window_long", 20)),
                    "momentum_weight_short": float(momentum_v2_cfg.get("momentum_weight_short", 0.6)),
                    "momentum_weight_long": float(momentum_v2_cfg.get("momentum_weight_long", 0.4)),
                    "hybrid_weight_momentum": float(momentum_v2_cfg.get("hybrid_weight_momentum", 1.0)),
                    "hybrid_weight_trend": float(momentum_v2_cfg.get("hybrid_weight_trend", 0.0)),
                    "rebalance_weekday": int(momentum_v2_cfg.get("rebalance_weekday", 1)),
                    "buy_filters": list(momentum_v2_cfg.get("buy_filters", MOMENTUM_V2_DEFAULTS["buy_filters"])),
                    "sell_signals": list(momentum_v2_cfg.get("sell_signals", MOMENTUM_V2_DEFAULTS["sell_signals"])),
                    "exit_ma_period": int(momentum_v2_cfg.get("exit_ma_period", 30)),
                },
            },
            {
                "id": MOMENTUM_STRATEGY_ID,
                "name": "Momentum TopN v1",
                "summary": "TopN momentum and trend hybrid ranking with weekly rebalance",
                "params": {
                    "n_short": int(momentum_v1_cfg.get("n_short", 5)),
                    "n_mid": int(momentum_v1_cfg.get("n_mid", 20)),
                    "n_long": int(momentum_v1_cfg.get("n_long", 40)),
                    "entry_threshold": float(momentum_v1_cfg.get("entry_threshold", 10.0)),
                    "max_holdings": int(momentum_v1_cfg.get("max_holdings", 5)),
                    "momentum_window_short": int(momentum_v1_cfg.get("momentum_window_short", 10)),
                    "momentum_window_long": int(momentum_v1_cfg.get("momentum_window_long", 20)),
                    "momentum_weight_short": float(momentum_v1_cfg.get("momentum_weight_short", 0.6)),
                    "momentum_weight_long": float(momentum_v1_cfg.get("momentum_weight_long", 0.4)),
                    "hybrid_weight_momentum": float(momentum_v1_cfg.get("hybrid_weight_momentum", 1.0)),
                    "hybrid_weight_trend": float(momentum_v1_cfg.get("hybrid_weight_trend", 0.0)),
                    "rebalance_weekday": int(momentum_v1_cfg.get("rebalance_weekday", 1)),
                    "buy_filters": list(momentum_v1_cfg.get("buy_filters", MOMENTUM_V1_DEFAULTS["buy_filters"])),
                    "sell_signals": list(momentum_v1_cfg.get("sell_signals", MOMENTUM_V1_DEFAULTS["sell_signals"])),
                },
            },
        ],
        "params_by_strategy": {
            TREND_STRATEGY_ID: deepcopy(trend_cfg),
            MOMENTUM_STRATEGY_ID: deepcopy(momentum_v1_cfg),
            MOMENTUM_STRATEGY_V2_ID: deepcopy(momentum_v2_cfg),
            MOMENTUM_STRATEGY_V3_ID: deepcopy(momentum_v3_cfg),
        },
    }
