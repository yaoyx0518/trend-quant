from __future__ import annotations

import numpy as np
import pandas as pd

from strategy.indicators import atr, efficiency_ratio


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value)
    except Exception:
        return default


def calculate_trend_score_snapshot(bars: pd.DataFrame, cfg: dict) -> dict:
    n_short = int(cfg.get("n_short", 5))
    n_mid = int(cfg.get("n_mid", 20))
    n_long = int(cfg.get("n_long", 40))
    atr_period = int(cfg.get("atr_period", 20))
    min_bars = max(n_long, atr_period) + 2

    if bars.empty or len(bars) < min_bars:
        return {
            "ok": False,
            "reason": "insufficient_bars",
            "trend_score": 0.0,
            "price_direction": 0.0,
            "confidence": 0.0,
            "atr": 0.0,
            "price": 0.0,
            "ma_mid": 0.0,
            "calc_details": {"rows": int(len(bars)), "required": int(min_bars)},
        }

    price = pd.to_numeric(bars["close"], errors="coerce")
    high = pd.to_numeric(bars["high"], errors="coerce")
    low = pd.to_numeric(bars["low"], errors="coerce")
    volume = pd.to_numeric(bars["volume"], errors="coerce").fillna(0.0)

    calc_df = pd.DataFrame(
        {"close": price, "high": high, "low": low, "volume": volume}
    ).dropna(subset=["close", "high", "low"])
    if len(calc_df) < min_bars:
        return {
            "ok": False,
            "reason": "invalid_bars_after_cleanup",
            "trend_score": 0.0,
            "price_direction": 0.0,
            "confidence": 0.0,
            "atr": 0.0,
            "price": 0.0,
            "ma_mid": 0.0,
            "calc_details": {"rows": int(len(calc_df)), "required": int(min_bars)},
        }

    atr_series = atr(calc_df, period=atr_period)
    atr_now = safe_float(atr_series.iloc[-1], default=0.0)
    if atr_now <= 0:
        return {
            "ok": False,
            "reason": "invalid_atr",
            "trend_score": 0.0,
            "price_direction": 0.0,
            "confidence": 0.0,
            "atr": 0.0,
            "price": safe_float(calc_df["close"].iloc[-1], 0.0),
            "ma_mid": 0.0,
            "calc_details": {"atr": atr_now},
        }

    weights_bias = np.array(
        [
            safe_float(cfg.get("w_bias_short", 0.4), 0.4),
            safe_float(cfg.get("w_bias_mid", 0.4), 0.4),
            safe_float(cfg.get("w_bias_long", 0.2), 0.2),
        ]
    )
    weights_slope = np.array(
        [
            safe_float(cfg.get("w_slope_short", 0.4), 0.4),
            safe_float(cfg.get("w_slope_mid", 0.4), 0.4),
            safe_float(cfg.get("w_slope_long", 0.2), 0.2),
        ]
    )

    bias_parts: list[float] = []
    slope_parts: list[float] = []
    close_series = calc_df["close"]

    for n in (n_short, n_mid, n_long):
        ma_n = close_series.rolling(n, min_periods=n).mean().iloc[-1]
        bias_n = (
            (close_series.iloc[-1] - ma_n) / atr_now if pd.notna(ma_n) else 0.0
        )
        ema_n = close_series.ewm(span=n, adjust=False).mean()
        slope_n = 0.0
        if len(ema_n) >= 2:
            slope_n = (ema_n.iloc[-1] - ema_n.iloc[-2]) / (atr_now * n)
        bias_parts.append(safe_float(bias_n))
        slope_parts.append(safe_float(slope_n))

    bias_mix = float(np.dot(weights_bias, np.array(bias_parts)))
    slope_mix = float(np.dot(weights_slope, np.array(slope_parts)))

    norm_bias = float(np.tanh(bias_mix / 2.0) * 100.0)
    norm_slope = float(np.tanh(slope_mix) * 100.0)

    w_bias_norm = safe_float(cfg.get("w_bias_norm", 0.5), 0.5)
    w_slope_norm = safe_float(cfg.get("w_slope_norm", 0.5), 0.5)
    price_direction = w_bias_norm * norm_bias + w_slope_norm * norm_slope

    vol_ma_period = int(cfg.get("vol_ma_period", 20))
    er_period = int(cfg.get("er_period", 10))

    vol_ma = safe_float(
        calc_df["volume"].rolling(vol_ma_period, min_periods=1).mean().iloc[-1], 0.0
    )
    current_volume = safe_float(calc_df["volume"].iloc[-1], 0.0)
    vol_ratio = (current_volume / vol_ma) if vol_ma > 0 else 0.0
    volume_factor = 1.0 if vol_ratio >= 3.0 else max(vol_ratio / 3.0, 0.0)

    er_series = efficiency_ratio(close_series, period=er_period)
    er_now = float(np.clip(safe_float(er_series.iloc[-1], 0.0), 0.0, 1.0))

    w_vol = safe_float(cfg.get("w_vol", 0.3), 0.3)
    w_er = safe_float(cfg.get("w_er", 0.7), 0.7)
    confidence = float((volume_factor**w_vol) * (er_now**w_er))
    trend_score = float(np.clip(price_direction * confidence, -100.0, 100.0))

    current_price = safe_float(close_series.iloc[-1], 0.0)
    ma_mid = safe_float(close_series.rolling(n_mid, min_periods=1).mean().iloc[-1], 0.0)

    calc_details = {
        "price": current_price,
        "ma_mid": ma_mid,
        "atr": atr_now,
        "bias_short": bias_parts[0],
        "bias_mid": bias_parts[1],
        "bias_long": bias_parts[2],
        "slope_short": slope_parts[0],
        "slope_mid": slope_parts[1],
        "slope_long": slope_parts[2],
        "bias_mix": bias_mix,
        "slope_mix": slope_mix,
        "norm_bias": norm_bias,
        "norm_slope": norm_slope,
        "vol_ma": vol_ma,
        "current_volume": current_volume,
        "vol_ratio": vol_ratio,
        "volume_factor": volume_factor,
        "er": er_now,
    }

    return {
        "ok": True,
        "reason": "ok",
        "trend_score": trend_score,
        "price_direction": price_direction,
        "confidence": confidence,
        "atr": atr_now,
        "price": current_price,
        "ma_mid": ma_mid,
        "calc_details": calc_details,
    }
