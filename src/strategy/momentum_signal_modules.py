from __future__ import annotations

from typing import Callable


BUY_FILTER_PRICE_ABOVE_MA20 = "price_above_ma20"
BUY_FILTER_PRICE_ABOVE_MA60 = "price_above_ma60"
BUY_FILTER_PRICE_ABOVE_MA200 = "price_above_ma200"
BUY_FILTER_TREND_SCORE_MAX = "trend_score_max"

SELL_SIGNAL_HARD_STOP = "hard_stop"
SELL_SIGNAL_CHANDELIER_STOP = "chandelier_stop"
SELL_SIGNAL_MA_BREAKDOWN_MAX = "ma_breakdown_max"

DEFAULT_MOMENTUM_BUY_FILTERS = [BUY_FILTER_PRICE_ABOVE_MA20]
DEFAULT_MOMENTUM_SELL_SIGNALS = [SELL_SIGNAL_HARD_STOP, SELL_SIGNAL_CHANDELIER_STOP]


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


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


def _details(signal: dict) -> dict:
    details = signal.get("calc_details", {})
    if isinstance(details, dict):
        return details
    return {}


def normalize_signal_modules(value: object, default: list[str]) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        raw_items = [str(item).strip() for item in value if str(item).strip()]
    else:
        text = str(value or "").strip()
        if text == "":
            raw_items = []
        else:
            normalized_text = text.replace("|", ",").replace(";", ",")
            raw_items = [part.strip() for part in normalized_text.split(",") if part.strip()]
    if not raw_items:
        raw_items = list(default)
    dedup: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        if item in seen:
            continue
        seen.add(item)
        dedup.append(item)
    return dedup


def buy_filter_price_above_ma20(signal: dict, strategy_cfg: dict) -> bool:
    _ = strategy_cfg
    details = _details(signal)
    price = _safe_float(details.get("price", 0.0), 0.0)
    ma20 = _safe_float(details.get("ma20", details.get("ma_mid", 0.0)), 0.0)
    return (price > 0) and (ma20 > 0) and (price > ma20)


def buy_filter_price_above_ma60(signal: dict, strategy_cfg: dict) -> bool:
    _ = strategy_cfg
    details = _details(signal)
    price = _safe_float(details.get("price", 0.0), 0.0)
    ma60 = _safe_float(details.get("ma60", 0.0), 0.0)
    return (price > 0) and (ma60 > 0) and (price > ma60)


def buy_filter_price_above_ma200(signal: dict, strategy_cfg: dict) -> bool:
    _ = strategy_cfg
    details = _details(signal)
    price = _safe_float(details.get("price", 0.0), 0.0)
    ma200 = _safe_float(details.get("ma200", 0.0), 0.0)
    return (price > 0) and (ma200 > 0) and (price > ma200)


def buy_filter_trend_score_max(signal: dict, strategy_cfg: dict) -> bool:
    max_entry_trend_score = _safe_float(strategy_cfg.get("max_entry_trend_score", 0.0), 0.0)
    if max_entry_trend_score <= 0:
        return False
    trend_score = signal.get("trend_score")
    if not _is_number(trend_score):
        return False
    return float(trend_score) <= max_entry_trend_score


BuyFilterHandler = Callable[[dict, dict], bool]
BUY_FILTER_REGISTRY: dict[str, BuyFilterHandler] = {
    BUY_FILTER_PRICE_ABOVE_MA20: buy_filter_price_above_ma20,
    BUY_FILTER_PRICE_ABOVE_MA60: buy_filter_price_above_ma60,
    BUY_FILTER_PRICE_ABOVE_MA200: buy_filter_price_above_ma200,
    BUY_FILTER_TREND_SCORE_MAX: buy_filter_trend_score_max,
}


SellSignalHandler = Callable[[dict, dict, dict, dict], tuple[bool, str, dict]]


def sell_signal_hard_stop(signal: dict, position: dict, state: dict, strategy_cfg: dict) -> tuple[bool, str, dict]:
    _ = strategy_cfg
    details = _details(signal)
    price = _safe_float(details.get("price", 0.0), 0.0)
    hard_stop_price = _safe_float(
        state.get("hard_stop_price", position.get("hard_stop_price", 0.0)), 0.0
    )
    triggered = (price > 0) and (hard_stop_price > 0) and (price < hard_stop_price)
    return triggered, SELL_SIGNAL_HARD_STOP, {}


def sell_signal_chandelier_stop(signal: dict, position: dict, state: dict, strategy_cfg: dict) -> tuple[bool, str, dict]:
    _ = strategy_cfg
    details = _details(signal)
    price = _safe_float(details.get("price", 0.0), 0.0)
    chandelier_stop_price = _safe_float(state.get("chandelier_stop_price", 0.0), 0.0)
    triggered = (price > 0) and (chandelier_stop_price > 0) and (price < chandelier_stop_price)
    return triggered, SELL_SIGNAL_CHANDELIER_STOP, {}


def sell_signal_ma_breakdown_max(signal: dict, position: dict, state: dict, strategy_cfg: dict) -> tuple[bool, str, dict]:
    _ = position
    _ = state
    if not _is_true(strategy_cfg.get("enable_ma_exit", True), True):
        return False, "", {}

    details = _details(signal)
    price = _safe_float(details.get("price", 0.0), 0.0)
    ma_candidates = {
        30: _safe_float(details.get("ma30", 0.0), 0.0),
        40: _safe_float(details.get("ma40", 0.0), 0.0),
        60: _safe_float(details.get("ma60", 0.0), 0.0),
    }
    valid_ma_candidates = {k: v for k, v in ma_candidates.items() if v > 0}
    if not valid_ma_candidates:
        return False, "", {}

    exit_ma_period = max(valid_ma_candidates, key=valid_ma_candidates.get)
    exit_ma_value = valid_ma_candidates.get(exit_ma_period, 0.0)
    triggered = (price > 0) and (exit_ma_value > 0) and (price < exit_ma_value)
    updates = {
        "exit_ma_candidates": valid_ma_candidates,
        "exit_ma_period": int(exit_ma_period),
        "exit_ma_value": float(exit_ma_value),
    }
    return triggered, f"ma{exit_ma_period}_breakdown_exit", updates


SELL_SIGNAL_REGISTRY: dict[str, SellSignalHandler] = {
    SELL_SIGNAL_HARD_STOP: sell_signal_hard_stop,
    SELL_SIGNAL_CHANDELIER_STOP: sell_signal_chandelier_stop,
    SELL_SIGNAL_MA_BREAKDOWN_MAX: sell_signal_ma_breakdown_max,
}

