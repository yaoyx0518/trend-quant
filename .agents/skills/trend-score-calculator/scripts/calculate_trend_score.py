#!/usr/bin/env python3
"""
Trend Score 计算器。
本脚本不提供数据获取功能，调用方需自行准备日K数据后传入计算。

输入：CSV 文件路径（需包含 time, open, high, low, close, volume 列）
输出：过去 N 个交易日的 Trend Score 表格

默认参数: ma_short=5, ma_mid=10, ma_long=20, 其他采用项目默认值。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def safe_float(value, default=0.0):
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value)
    except Exception:
        return default


def atr(df: pd.DataFrame, period: int = 20) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift(1)).abs()
    low_close = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()


def efficiency_ratio(series: pd.Series, period: int = 10) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=float)
    change = (series - series.shift(period)).abs()
    volatility = series.diff().abs().rolling(period, min_periods=1).sum()
    er = change / volatility.replace(0, np.nan)
    return er.fillna(0.0)


def calculate_trend_score(bars: pd.DataFrame, cfg: dict) -> dict:
    n_short = int(cfg.get("n_short", 5))
    n_mid = int(cfg.get("n_mid", 10))
    n_long = int(cfg.get("n_long", 20))
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

    return {
        "ok": True,
        "reason": "ok",
        "trend_score": trend_score,
        "price_direction": price_direction,
        "confidence": confidence,
        "atr": atr_now,
        "price": current_price,
        "ma_mid": ma_mid,
    }


def load_bars(csv_path: str) -> pd.DataFrame:
    """从 CSV 加载日K数据。支持通过 stdin 传入 '-' 作为路径。"""
    if csv_path == "-":
        df = pd.read_csv(sys.stdin)
    else:
        df = pd.read_csv(csv_path)

    required = {"time", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV 缺少必要列: {missing}")

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["time", "close", "high", "low"]).sort_values("time").reset_index(drop=True)
    return df


def main():
    if len(sys.argv) < 2:
        print("Usage: python calculate_trend_score.py <csv_path> [days=5]")
        print("  csv_path: 日K数据 CSV 文件路径，列需包含 time,open,high,low,close,volume")
        print("  传 '-' 表示从标准输入读取 CSV")
        sys.exit(1)

    csv_path = sys.argv[1]
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    cfg = {
        "n_short": 5,
        "n_mid": 10,
        "n_long": 20,
        "atr_period": 20,
        "w_bias_short": 0.4,
        "w_bias_mid": 0.4,
        "w_bias_long": 0.2,
        "w_slope_short": 0.4,
        "w_slope_mid": 0.4,
        "w_slope_long": 0.2,
        "w_bias_norm": 0.5,
        "w_slope_norm": 0.5,
        "vol_ma_period": 20,
        "er_period": 10,
        "w_vol": 0.3,
        "w_er": 0.7,
    }

    try:
        df = load_bars(csv_path)
    except Exception as e:
        print(f"数据加载失败: {e}")
        sys.exit(1)

    min_bars = max(cfg["n_long"], cfg["atr_period"]) + 2
    if len(df) < min_bars + days:
        print(f"历史数据不足，需要至少 {min_bars + days} 天，实际只有 {len(df)} 天")
        sys.exit(1)

    results = []
    total_rows = len(df)
    for i in range(days):
        idx = total_rows - days + i
        bars = df.iloc[: idx + 1].copy()
        result = calculate_trend_score(bars, cfg)

        trade_date = pd.to_datetime(bars.iloc[-1]["time"]).strftime("%Y-%m-%d")
        results.append(
            {
                "date": trade_date,
                "close": round(result["price"], 3) if result["ok"] else None,
                "trend_score": round(result["trend_score"], 2) if result["ok"] else None,
                "price_direction": round(result["price_direction"], 2) if result["ok"] else None,
                "confidence": round(result["confidence"], 4) if result["ok"] else None,
                "atr": round(result["atr"], 4) if result["ok"] else None,
                "ma10": round(result["ma_mid"], 3) if result["ok"] else None,
            }
        )

    print(f"DAYS={days}")
    print(f"ROWS={len(df)}")
    for r in results:
        print(
            f"{r['date']}\t"
            f"{r['close']}\t"
            f"{r['trend_score']}\t"
            f"{r['price_direction']}\t"
            f"{r['confidence']}\t"
            f"{r['atr']}\t"
            f"{r['ma10']}"
        )


if __name__ == "__main__":
    main()
