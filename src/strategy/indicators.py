from __future__ import annotations

import pandas as pd

from core.indicators import atr as _core_atr
from core.indicators import efficiency_ratio as _core_er


def atr(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """Backward-compatible shim — canonical implementation is core.indicators.atr."""
    return _core_atr(df, period=period)


def efficiency_ratio(series: pd.Series, period: int = 10) -> pd.Series:
    """Backward-compatible shim — canonical implementation is core.indicators.efficiency_ratio."""
    return _core_er(series, period=period)
