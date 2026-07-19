"""Canonical strategy/indicator parameters.

Single source of truth is the ``app_config`` DB table (key ``strategy``);
the code defaults below are the fallback and the seed for fresh databases.
Only live keys are kept here — parameters of the retired legacy engines
(momentum sections, entry thresholds, fee/slippage, lookback_days, ...)
were dropped during the storage consolidation.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

DEFAULT_STRATEGY_CONFIG: dict[str, Any] = {
    "adjust": "qfq",
    "n_short": 5,
    "n_mid": 10,
    "n_long": 20,
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
    "atr_period": 20,
    "hard_stop_atr_mul_default": 1.5,
    "chandelier_stop_atr_mul": 2.5,
    "backtest_start_primary": "2025-01-01",
}

_LIVE_KEYS = frozenset(DEFAULT_STRATEGY_CONFIG)
_CONFIG_KEY = "strategy"
_LEGACY_YAML = "config/strategy.yaml"


def get_strategy_config() -> dict[str, Any]:
    """Return the strategy config: DB value, lazily seeded from defaults.

    Falls back to the code defaults when the database is unavailable
    (bare test/script contexts).
    """
    from data.storage.db import get_db

    try:
        db = get_db()
        stored = db.get_config(_CONFIG_KEY, default=None)
        if isinstance(stored, dict):
            cfg = dict(DEFAULT_STRATEGY_CONFIG)
            cfg.update({k: v for k, v in stored.items() if k in _LIVE_KEYS})
            return cfg
        # Fresh database: seed it (legacy yaml values overlay the defaults).
        db.set_config(_CONFIG_KEY, _seed_payload())
        return get_strategy_config()
    except (RuntimeError, sqlite3.Error):
        return dict(DEFAULT_STRATEGY_CONFIG)


def _seed_payload() -> dict[str, Any]:
    cfg = dict(DEFAULT_STRATEGY_CONFIG)
    path = Path(_LEGACY_YAML)
    if not path.exists():
        return cfg
    try:
        import yaml

        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        legacy = payload.get("strategy", {}) if isinstance(payload, dict) else {}
        if isinstance(legacy, dict):
            cfg.update({k: v for k, v in legacy.items() if k in _LIVE_KEYS})
    except Exception:
        pass
    return cfg
