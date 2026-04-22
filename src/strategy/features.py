from __future__ import annotations

import pandas as pd

from strategy.trend_score_core import calculate_trend_score_snapshot, safe_float


def _momentum(close_series: pd.Series, window: int) -> float | None:
    w = int(window)
    if w <= 0 or close_series.empty or len(close_series) <= w:
        return None
    latest = safe_float(close_series.iloc[-1], 0.0)
    prev = safe_float(close_series.iloc[-1 - w], 0.0)
    if latest <= 0 or prev <= 0:
        return None
    return (latest / prev) - 1.0


def _ma(close_series: pd.Series, window: int) -> float | None:
    w = int(window)
    if w <= 0 or len(close_series) < w:
        return None
    ma_value = close_series.rolling(w, min_periods=w).mean().iloc[-1]
    return safe_float(ma_value, 0.0)


def build_trend_score_features(bars: pd.DataFrame, state: dict, cfg: dict) -> dict:
    snapshot = calculate_trend_score_snapshot(bars=bars, cfg=cfg)
    calc_details = dict(snapshot.get("calc_details", {}))
    calc_details.update(
        {
            "prev_prev_score": safe_float(state.get("prev_prev_trend_score", 0.0), 0.0),
            "prev_score": safe_float(state.get("prev_trend_score", 0.0), 0.0),
            "position_qty": int(state.get("position_qty", 0) or 0),
            "sellable_qty": int(state.get("sellable_qty", 0) or 0),
            "hard_stop_price": safe_float(state.get("hard_stop_price", 0.0), 0.0),
            "chandelier_stop_price": safe_float(state.get("chandelier_stop_price", 0.0), 0.0),
        }
    )
    return {
        "ok": bool(snapshot.get("ok", False)),
        "reason": str(snapshot.get("reason", "invalid_trend_snapshot")),
        "trend_score": float(snapshot.get("trend_score", 0.0)),
        "price_direction": float(snapshot.get("price_direction", 0.0)),
        "confidence": float(snapshot.get("confidence", 0.0)),
        "calc_details": calc_details,
    }


def build_momentum_features(bars: pd.DataFrame, state: dict, cfg: dict) -> dict:
    snapshot = calculate_trend_score_snapshot(bars=bars, cfg=cfg)
    close_series = pd.to_numeric(bars.get("close"), errors="coerce").dropna()
    mom_short = _momentum(close_series, int(cfg.get("momentum_window_short", 10)))
    mom_long = _momentum(close_series, int(cfg.get("momentum_window_long", 20)))

    w_short = safe_float(cfg.get("momentum_weight_short", 0.6), 0.6)
    w_long = safe_float(cfg.get("momentum_weight_long", 0.4), 0.4)
    momentum_mix = None
    if mom_short is not None and mom_long is not None:
        momentum_mix = w_short * mom_short + w_long * mom_long

    calc_details = dict(snapshot.get("calc_details", {}))
    calc_details.update(
        {
            "momentum_short": mom_short,
            "momentum_long": mom_long,
            "momentum_mix": momentum_mix,
            "ma20": _ma(close_series, 20),
            "ma30": _ma(close_series, 30),
            "ma40": _ma(close_series, 40),
            "ma60": _ma(close_series, 60),
            "ma200": _ma(close_series, 200),
            "hybrid_score": None,
            "trend_rank_z": None,
            "momentum_rank_z": None,
            "hard_stop_price": safe_float(state.get("hard_stop_price", 0.0), 0.0),
            "chandelier_stop_price": safe_float(state.get("chandelier_stop_price", 0.0), 0.0),
            "position_qty": int(state.get("position_qty", 0) or 0),
            "sellable_qty": int(state.get("sellable_qty", 0) or 0),
            "prev_score": safe_float(state.get("prev_trend_score", 0.0), 0.0),
        }
    )
    return {
        "ok": bool(snapshot.get("ok", False)),
        "reason": str(snapshot.get("reason", "invalid_trend_snapshot")),
        "trend_score": float(snapshot.get("trend_score", 0.0)),
        "price_direction": float(snapshot.get("price_direction", 0.0)),
        "confidence": float(snapshot.get("confidence", 0.0)),
        "momentum_short": mom_short,
        "momentum_long": mom_long,
        "momentum_mix": momentum_mix,
        "momentum_score": None,
        "hybrid_score": None,
        "calc_details": calc_details,
    }
