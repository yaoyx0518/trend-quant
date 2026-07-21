"""Unified technical indicator library — the single implementation project-wide.

All functions are vectorized: pandas Series/DataFrame in, Series/DataFrame out.

Locked semantics (master plan v1.1):
- RSI: Wilder smoothing (alpha = 1/period)
- MACD histogram: (DIF - DEA) * 2  (China charting convention)
- BIAS: decimal ratio (presentation layer multiplies by 100 when needed)

Only price/volume-derived deterministic indicators belong here; anything
stochastic or non-price-derived (e.g. random_uniform) must never be cached
or unified into this module.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Bump when any formula in this module changes; indicator cache tables keyed
# by this version are rebuilt at startup (see data/indicator_store, future P1).
INDICATOR_FORMULA_VERSION = 1


def sma(series: pd.Series, period: int, min_periods: int | None = None) -> pd.Series:
    """Simple moving average; by default requires a full window."""
    if series.empty:
        return pd.Series(dtype=float)
    return series.rolling(period, min_periods=min_periods or period).mean()


def ema(series: pd.Series, span: int, min_periods: int = 0) -> pd.Series:
    """Exponential moving average (adjust=False).

    ``min_periods=0`` keeps warmup values (backtest behavior);
    ``min_periods=span`` suppresses the warmup region (chart behavior).
    """
    if series.empty:
        return pd.Series(dtype=float)
    return series.ewm(span=span, adjust=False, min_periods=min_periods).mean()


def atr(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """Average True Range, SMA-smoothed with warmup allowed (min_periods=1)."""
    if df.empty:
        return pd.Series(dtype=float)
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()


def efficiency_ratio(series: pd.Series, period: int = 10) -> pd.Series:
    """Kaufman efficiency ratio: |net change| / sum of |steps|."""
    if series.empty:
        return pd.Series(dtype=float)
    change = (series - series.shift(period)).abs()
    volatility = series.diff().abs().rolling(period, min_periods=1).sum()
    er = change / volatility.replace(0, np.nan)
    return er.fillna(0.0)


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder-smoothed RSI.

    Boundary rules (match the chart implementation): avg_loss == 0 with
    avg_gain > 0 -> 100; both zero -> 50.
    """
    if close.empty:
        return pd.Series(dtype=float)
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    out = out.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    out = out.mask((avg_loss == 0) & (avg_gain == 0), 50.0)
    return out


def macd(
    close: pd.Series,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
    *,
    warmup: bool = True,
) -> pd.DataFrame:
    """MACD with histogram = (DIF - DEA) * 2.

    ``warmup=True`` starts the EMAs from the first bar (backtest behavior);
    ``warmup=False`` suppresses each EMA until its span is complete (chart
    behavior).
    """
    if close.empty:
        return pd.DataFrame({"dif": pd.Series(dtype=float), "dea": pd.Series(dtype=float), "hist": pd.Series(dtype=float)})
    fast = ema(close, fast_period, min_periods=0 if warmup else fast_period)
    slow = ema(close, slow_period, min_periods=0 if warmup else slow_period)
    dif = fast - slow
    dea = ema(dif, signal_period, min_periods=0 if warmup else signal_period)
    hist = (dif - dea) * 2
    return pd.DataFrame({"dif": dif, "dea": dea, "hist": hist})


def bollinger(close: pd.Series, period: int = 20, std_mul: float = 2.0) -> pd.DataFrame:
    """Bollinger bands with population std (ddof=0)."""
    if close.empty:
        return pd.DataFrame({"mid": pd.Series(dtype=float), "up": pd.Series(dtype=float), "dn": pd.Series(dtype=float)})
    mid = close.rolling(period, min_periods=period).mean()
    std = close.rolling(period, min_periods=period).std(ddof=0)
    return pd.DataFrame({"mid": mid, "up": mid + std_mul * std, "dn": mid - std_mul * std})


def bias(close: pd.Series, period: int = 20) -> pd.Series:
    """(close - SMA(period)) / SMA(period) as a decimal ratio."""
    ma = sma(close, period)
    return (close - ma) / ma


def momentum_return(series: pd.Series, period: int = 20) -> pd.Series:
    """Simple N-period return: series / series.shift(period) - 1."""
    if series.empty:
        return pd.Series(dtype=float)
    return series / series.shift(period) - 1.0
