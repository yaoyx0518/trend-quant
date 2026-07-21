"""Root fixtures shared across all test layers.

Provides:
- Sample OHLCV bar data factories (bull / bear / sideways / short)
- Sample quote factory
- Test database path and Database instance
- Complete isolated test environment
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Sample bar‑data factories
# ---------------------------------------------------------------------------

def make_bull_bars(n_days: int = 30) -> pd.DataFrame:
    """Generate synthetic bullish OHLCV data (steady uptrend).

    Returns a DataFrame with columns: time, open, high, low, close, volume.
    """
    rng = np.random.default_rng(42)
    base = date(2025, 1, 6)  # a Monday
    records: list[dict[str, Any]] = []
    price = 10.0
    for i in range(n_days):
        day = base + pd.Timedelta(days=i)
        if day.weekday() >= 5:
            continue  # skip weekends for realism
        # 2 % daily drift upward + 1 % noise
        change = 0.02 + rng.normal(0, 0.01)
        close = price * (1 + change)
        high = close * (1 + abs(rng.normal(0, 0.005)))
        low = close * (1 - abs(rng.normal(0, 0.005)))
        open_ = price * (1 + rng.normal(0, 0.003))
        volume = rng.integers(500_000, 2_000_000)
        records.append(
            {
                "time": day.isoformat(),
                "open": round(float(open_), 4),
                "high": round(float(high), 4),
                "low": round(float(low), 4),
                "close": round(float(close), 4),
                "volume": int(volume),
            }
        )
        price = close
    return pd.DataFrame(records)


def make_bear_bars(n_days: int = 30) -> pd.DataFrame:
    """Generate synthetic bearish OHLCV data (steady downtrend)."""
    rng = np.random.default_rng(99)
    base = date(2025, 1, 6)
    records: list[dict[str, Any]] = []
    price = 100.0
    for i in range(n_days):
        day = base + pd.Timedelta(days=i)
        if day.weekday() >= 5:
            continue
        change = -0.02 + rng.normal(0, 0.01)
        close = price * (1 + change)
        high = close * (1 + abs(rng.normal(0, 0.005)))
        low = close * (1 - abs(rng.normal(0, 0.005)))
        open_ = price * (1 + rng.normal(0, 0.003))
        volume = rng.integers(500_000, 2_000_000)
        records.append(
            {
                "time": day.isoformat(),
                "open": round(float(open_), 4),
                "high": round(float(high), 4),
                "low": round(float(low), 4),
                "close": round(float(close), 4),
                "volume": int(volume),
            }
        )
        price = close
    return pd.DataFrame(records)


def make_sideways_bars(n_days: int = 30) -> pd.DataFrame:
    """Generate synthetic sideways OHLCV data (range-bound)."""
    rng = np.random.default_rng(7)
    base = date(2025, 1, 6)
    records: list[dict[str, Any]] = []
    price = 50.0
    for i in range(n_days):
        day = base + pd.Timedelta(days=i)
        if day.weekday() >= 5:
            continue
        change = rng.normal(0, 0.005)  # near-zero drift
        close = price * (1 + change)
        high = close * (1 + abs(rng.normal(0, 0.004)))
        low = close * (1 - abs(rng.normal(0, 0.004)))
        open_ = price * (1 + rng.normal(0, 0.002))
        volume = rng.integers(300_000, 1_500_000)
        records.append(
            {
                "time": day.isoformat(),
                "open": round(float(open_), 4),
                "high": round(float(high), 4),
                "low": round(float(low), 4),
                "close": round(float(close), 4),
                "volume": int(volume),
            }
        )
        price = close
    return pd.DataFrame(records)


def make_short_bars(n_days: int = 5) -> pd.DataFrame:
    """Generate bars insufficient for trend-score computation (< 22 bars)."""
    rng = np.random.default_rng(1)
    base = date(2025, 1, 6)
    records: list[dict[str, Any]] = []
    price = 10.0
    for i in range(n_days):
        day = base + pd.Timedelta(days=i)
        if day.weekday() >= 5:
            continue
        change = rng.normal(0.01, 0.01)
        close = price * (1 + change)
        records.append(
            {
                "time": day.isoformat(),
                "open": round(float(price), 4),
                "high": round(float(close * 1.01), 4),
                "low": round(float(close * 0.99), 4),
                "close": round(float(close), 4),
                "volume": 1_000_000,
            }
        )
        price = close
    return pd.DataFrame(records)


def make_flat_bars(n_days: int = 30) -> pd.DataFrame:
    """Generate bars with constant price (ATR = 0)."""
    base = date(2025, 1, 6)
    records: list[dict[str, Any]] = []
    for i in range(n_days):
        day = base + pd.Timedelta(days=i)
        if day.weekday() >= 5:
            continue
        records.append(
            {
                "time": day.isoformat(),
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "volume": 1_000_000,
            }
        )
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Quote factory
# ---------------------------------------------------------------------------

def make_quote(
    symbol: str = "510300.SS",
    price: float = 4.50,
    volume: int = 5_000_000,
) -> dict[str, Any]:
    """Return a standardised real‑time quote dict."""
    return {
        "symbol": symbol,
        "name": "沪深300ETF",
        "price": price,
        "open": price * 0.99,
        "high": price * 1.02,
        "low": price * 0.98,
        "pre_close": price * 0.995,
        "volume": volume,
        "amount": price * volume,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# Test‑database fixtures
# ---------------------------------------------------------------------------

@dataclass
class TestEnvironment:
    """Container for all paths in an isolated test environment."""

    root: Path
    db_path: Path
    market_dir: Path
    runtime_dir: Path
    calc_log_path: Path
    config_dir: Path


@pytest.fixture
def test_db_path(tmp_path: Path) -> Path:
    """A unique SQLite database path inside *tmp_path*."""
    db_path = tmp_path / "data" / "test_trend_quant.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path


@pytest.fixture
def test_db(test_db_path: Path):
    """Return an isolated :class:`Database` instance.

    Does NOT mutate the global ``get_db`` singleton — each test gets
    its own Database object.
    """
    from data.storage.db import Database

    return Database(test_db_path)


@pytest.fixture
def test_env(tmp_path: Path, test_db) -> TestEnvironment:
    """Complete isolated directory tree + database.

    Creates the standard directory layout under *tmp_path* so
    integration tests can write files without touching production data.
    """
    market_dir = tmp_path / "data" / "market" / "etf"
    runtime_dir = tmp_path / "data" / "runtime_test"
    calc_log_dir = tmp_path / "logs" / "calc"
    config_dir = tmp_path / "config"

    for d in (market_dir, runtime_dir, calc_log_dir, config_dir):
        d.mkdir(parents=True, exist_ok=True)

    return TestEnvironment(
        root=tmp_path,
        db_path=test_db_path,
        market_dir=market_dir,
        runtime_dir=runtime_dir,
        calc_log_path=calc_log_dir / "calc.jsonl",
        config_dir=config_dir,
    )


# ---------------------------------------------------------------------------
# Default strategy config (matches core/strategy_config.py defaults)
# ---------------------------------------------------------------------------

@pytest.fixture
def default_cfg() -> dict:
    """Return a strategy configuration dict with default parameters."""
    from core.strategy_config import DEFAULT_STRATEGY_CONFIG

    return dict(DEFAULT_STRATEGY_CONFIG)
