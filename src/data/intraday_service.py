"""Intraday (real-time) market data service.

All functions operate **in-memory only** — they never write to the
database.  The synthetic intraday bars are constructed from TickFlow
real-time quotes and merged with historical daily bars for trend-score
calculation.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from math import isfinite
from typing import Any, Callable

import numpy as np
import pandas as pd

from data.storage.db import Database
from data.service import DataService
from core.indicators import atr as _compute_atr
from core.trend import calculate_trend_score_snapshot, safe_float

# ---------------------------------------------------------------------------
# synthetic bar construction
# ---------------------------------------------------------------------------


def _number(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def _detect_trend_phase(
    trend_scores: list[float | None],
    trend_ma5: list[float | None],
    closes: list[float | None],
    dates: list[str],
) -> dict:
    """Detect the current trend phase and its start date.

    Uses the last bar's trend_score and MA5 to determine the current
    phase ("start" / "end"), then walks backwards to find the transition
    bar where this phase began.  Returns None if the current bar is in
    neither state.

    - 趋势启动 (start): trend_score >= 5  AND  trend_ma5 >= 0
    - 趋势结束 (end):   trend_score <= -5 AND  trend_ma5 <= 0

    Returns a dict with keys:
      phase: "start" | "end" | None
      days:  int (the transition bar is day 1)
      change_pct: float (transition close → latest close % change)
      signal_date: str (ISO date of the transition bar)
    """
    default: dict = {
        "phase": None,
        "days": None,
        "change_pct": None,
        "signal_date": None,
    }
    n = len(trend_scores)
    if n < 5:
        return default

    # Scan backwards from the latest bar to find the most recent signal.
    # Skip bars that are NEITHER (don't satisfy either condition).
    latest_idx = n - 1
    phase = None
    scan_idx = -1
    for i in range(n - 1, 3, -1):
        ts = trend_scores[i]
        ma5 = trend_ma5[i]
        if ts is None or ma5 is None:
            continue
        if ts >= 5 and ma5 >= 0:
            phase = "start"
            scan_idx = i
            break
        if ts <= -5 and ma5 <= 0:
            phase = "end"
            scan_idx = i
            break
    if phase is None:
        return default  # no signal found at all

    latest_close = closes[latest_idx] if latest_idx < len(closes) else None
    if latest_close is None or latest_close <= 0:
        return default

    # Walk backwards from scan_idx to find where this phase started
    # (the transition point — first bar where the condition became true).
    signal_idx = scan_idx
    for j in range(scan_idx - 1, 3, -1):
        prev_ts = trend_scores[j]
        prev_ma5 = trend_ma5[j]
        if prev_ts is None or prev_ma5 is None:
            break
        if phase == "start":
            if prev_ts >= 5 and prev_ma5 >= 0:
                signal_idx = j  # still in same phase — move start earlier
            else:
                break  # phase started at signal_idx
        else:  # phase == "end"
            if prev_ts <= -5 and prev_ma5 <= 0:
                signal_idx = j
            else:
                break

    signal_close = closes[signal_idx] if signal_idx < len(closes) else None
    if signal_close is None or signal_close <= 0:
        return default

    # Days: transition bar is day 1, counted to the latest bar.
    days = latest_idx - signal_idx + 1
    change_pct = round((latest_close / signal_close - 1.0) * 100.0, 2)
    signal_date = dates[signal_idx] if signal_idx < len(dates) else None

    return {
        "phase": phase,
        "days": days,
        "change_pct": change_pct,
        "signal_date": signal_date,
    }


def build_synthetic_bar(quote: dict, prev_volume: float) -> dict:
    """Build a single synthetic daily bar from a real-time quote.

    Parameters
    ----------
    quote: TickFlow quote dict with keys ``open, high, low, price``.
    prev_volume: previous trading day's volume (used as today's
        approximate volume because intraday volume is incomplete).

    Returns
    -------
    dict with keys ``time, open, high, low, close, volume, amount``
    suitable for appending to a historical daily-bar DataFrame.
    """
    quote_open = _number(quote.get("open")) or 0.0
    quote_high = _number(quote.get("high")) or 0.0
    quote_low = _number(quote.get("low")) or 0.0
    quote_price = _number(quote.get("price")) or 0.0
    quote_amount = _number(quote.get("amount")) or 0.0

    return {
        "time": datetime.now(),
        "open": quote_open,
        "high": max(quote_high, quote_open, quote_price),
        "low": min(quote_low, quote_open, quote_price) if quote_low > 0 else min(quote_open, quote_price),
        "close": quote_price,
        "volume": float(prev_volume) if prev_volume > 0 else 0.0,
        "amount": quote_amount,
    }


# ---------------------------------------------------------------------------
# intraday trend score
# ---------------------------------------------------------------------------


def compute_intraday_trend_score(
    historical_bars: pd.DataFrame,
    quote: dict,
    trend_config: dict,
) -> dict:
    """Calculate a trend-score snapshot for a symbol using intraday data.

    1. Compute ATR from *historical_bars only* (no synthetic bar).
    2. Use the previous day's volume as *fixed_volume*.
    3. Append the synthetic intraday bar to historical bars.
    4. Call ``calculate_trend_score_snapshot`` with ``fixed_atr``
       and ``fixed_volume`` so that today's incomplete bar does not
       contaminate ATR or volume-ratio calculations.

    Parameters
    ----------
    historical_bars: DataFrame of daily bars (no intraday bar).
        Must contain ``open, high, low, close, volume``.
    quote: real-time quote dict from TickFlow.
    trend_config: strategy configuration dict.

    Returns
    -------
    Same structure as ``calculate_trend_score_snapshot`` with an
    additional key ``is_intraday: True``.
    """
    min_bars = max(
        int(trend_config.get("n_long", 20)),
        int(trend_config.get("atr_period", 20)),
    ) + 2

    if historical_bars.empty or len(historical_bars) < min_bars - 1:
        return {
            "ok": False,
            "reason": "insufficient_historical_bars",
            "trend_score": 0.0,
            "price_direction": 0.0,
            "confidence": 0.0,
            "atr": 0.0,
            "price": _number(quote.get("price")) or 0.0,
            "ma_mid": 0.0,
            "is_intraday": True,
            "calc_details": {
                "rows": int(len(historical_bars)),
                "required": int(min_bars),
            },
        }

    # Compute ATR from historical bars ONLY (no synthetic bar).
    atr_series = _compute_atr(historical_bars, period=int(trend_config.get("atr_period", 20)))
    fixed_atr = safe_float(atr_series.iloc[-1], default=0.0)

    # Previous day's volume.
    prev_volume = safe_float(historical_bars["volume"].iloc[-1], default=0.0)

    # Build synthetic bar.
    synth = build_synthetic_bar(quote, prev_volume)
    synth_row = pd.DataFrame([synth])

    # Concatenate: historical + synthetic bar.
    combined = pd.concat([historical_bars, synth_row], ignore_index=True)

    # Compute trend score with fixed ATR and fixed volume.
    result = calculate_trend_score_snapshot(
        combined, trend_config,
        fixed_atr=fixed_atr if fixed_atr > 0 else None,
        fixed_volume=prev_volume,
    )
    result["is_intraday"] = True
    return result


# ---------------------------------------------------------------------------
# batch intraday dashboard
# ---------------------------------------------------------------------------


def _ma5(values: list[float | None]) -> list[float | None]:
    series = pd.Series(values, dtype="float64").rolling(5, min_periods=5).mean()
    return [_number(v) for v in series]


def _strength(values: list[float], value: float | None) -> int | None:
    if value is None or not values:
        return None
    return round(sum(score <= value for score in values) * 100 / len(values))


def _priority(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 999999


def _key_tuple(key: object) -> tuple[object, ...]:
    return key if isinstance(key, tuple) else (key,)


def build_intraday_dashboard(
    symbols: list[str],
    db: Database,
    data_service: DataService,
    trend_config: dict,
    *,
    progress_callback: Callable[[dict], None] | None = None,
) -> dict:
    """Build a full intraday subject-market dashboard for *symbols*.

    Orchestrates:
    1. Batch real-time quote fetch (chunked, with progress).
    2. Per-symbol historical bar load + intraday trend-score compute.
    3. Multi-level aggregation matching the EOD dashboard structure.

    Returns a dict with the same keys as ``build_subject_dashboard_payload``
    plus ``is_intraday: True`` and ``intraday_ts``.
    """
    total = len(symbols)
    if total == 0:
        return {
            "as_of": None,
            "groups": [],
            "secondary_count": 0,
            "category_count": 0,
            "instrument_count": 0,
            "is_intraday": True,
            "intraday_ts": datetime.now().isoformat(),
        }

    def _progress(stage: str, percent: float, message: str = "") -> None:
        if progress_callback:
            progress_callback({"stage": stage, "percent": percent, "message": message})

    _progress("quotes", 0.0, f"正在获取 {total} 个标的实时报价…")

    # --- 1. Batch quotes ---------------------------------------------------
    quotes = data_service.fetch_latest_quotes(symbols)
    quote_ok = sum(1 for q in quotes.values() if "error" not in q and q.get("price") is not None)
    _progress("quotes", 0.15, f"实时报价: {quote_ok}/{total} 成功")

    # --- 2. Load metadata --------------------------------------------------
    metadata_map = db.get_instrument_metadata_map()
    name_map: dict[str, str] = {}
    for sym, meta in metadata_map.items():
        name = str(meta.get("name", "")).strip()
        if name:
            name_map[sym] = name

    # --- 3. Per-symbol computation -----------------------------------------
    instrument_rows: list[dict] = []
    failed: list[str] = []

    for idx, symbol in enumerate(symbols):
        percent = 0.15 + 0.70 * (idx + 1) / total
        _progress("compute", percent, f"计算趋势值 {idx + 1}/{total}: {symbol}")

        quote = quotes.get(symbol, {})
        if not quote or "error" in quote or quote.get("price") is None:
            failed.append(symbol)
            continue

        meta = metadata_map.get(symbol, {})
        hist = db.load_market_data(symbol, price_mode="qfq")
        if hist.empty:
            failed.append(symbol)
            continue

        # Ensure correct types and sorting.
        hist["time"] = pd.to_datetime(hist["time"], errors="coerce")
        hist = hist.dropna(subset=["time", "open", "high", "low", "close"]).sort_values("time").reset_index(drop=True)
        for col in ("open", "high", "low", "close", "volume", "amount"):
            if col in hist.columns:
                hist[col] = pd.to_numeric(hist[col], errors="coerce")

        if len(hist) < 20:
            failed.append(symbol)
            continue

        result = compute_intraday_trend_score(hist, quote, trend_config)
        if not result.get("ok"):
            failed.append(symbol)
            continue

        # --- Compute trend history for phase detection -----------------
        # Late import to avoid circular dependency (market_view → intraday_service).
        from app.routers.market_view import compute_trend_indicator  # noqa: PLC0415

        trend_result = compute_trend_indicator(hist, trend_config)
        hist_trend_scores = trend_result.get("score", [])
        hist_closes = [_number(v) for v in hist["close"]]
        hist_dates = [pd.Timestamp(v).date().isoformat() for v in hist["time"]]

        # For intraday, extend the historical series with the intraday
        # snapshot so the current phase is determined by the live trend
        # score, not yesterday's.
        intraday_ts = result["trend_score"]
        intraday_price = _number(quote.get("price")) or 0.0
        extended_scores = list(hist_trend_scores) + [intraday_ts]
        extended_ma5 = _ma5(extended_scores)
        extended_closes = list(hist_closes) + [intraday_price]
        extended_dates = list(hist_dates) + [datetime.now().date().isoformat()]

        phase_info = _detect_trend_phase(
            extended_scores, extended_ma5,
            extended_closes, extended_dates,
        )
        # Override change_pct with intraday price if phase detected.
        if phase_info["phase"] is not None and intraday_price > 0:
            # Re-lookup signal close from original hist_closes.
            sig_date = phase_info["signal_date"]
            signal_idx = None
            for i in range(len(hist_dates) - 1, -1, -1):
                if hist_dates[i] == sig_date:
                    signal_idx = i
                    break
            if signal_idx is not None and signal_idx < len(hist_closes):
                sig_close = hist_closes[signal_idx]
                if sig_close and sig_close > 0:
                    phase_info["change_pct"] = round((intraday_price / sig_close - 1.0) * 100.0, 2)

        name = str(meta.get("name") or name_map.get(symbol, "")).strip()
        instrument_rows.append(
            {
                "symbol": symbol,
                "name": name or symbol,
                "time": datetime.now(),
                "trend_score": result["trend_score"],
                "return_1d": _number(quote.get("price")) or 0.0,
                "return_5d": 0.0,  # will be computed from history below
                "return_20d": 0.0,
                "return_60d": 0.0,
                "open": _number(quote.get("open")) or 0.0,
                "high": _number(quote.get("high")) or 0.0,
                "low": _number(quote.get("low")) or 0.0,
                "close": _number(quote.get("price")) or 0.0,
                "volume": safe_float(hist["volume"].iloc[-1], 0.0),
                "amount": _number(quote.get("amount")) or 0.0,
                "category_l1": str(meta.get("category_l1") or ""),
                "category_l2": str(meta.get("category_l2") or ""),
                "category_l3": str(meta.get("category_l3") or ""),
                "priority_l1": int(meta.get("priority_l1") or 9999),
                "priority_l2": int(meta.get("priority_l2") or 9999),
                "priority_l3": int(meta.get("priority_l3") or 9999),
                "sort_order": int(meta.get("sort_order") or 999999),
                "is_intraday": True,
                "trend_phase": phase_info["phase"],
                "trend_phase_days": phase_info["days"],
                "trend_phase_change_pct": phase_info["change_pct"],
                "trend_phase_signal_date": phase_info["signal_date"],
            }
        )

    _progress("aggregate", 0.90, f"正在聚合 {len(instrument_rows)} 个标的…")

    # --- 4. Compute multi-period returns -----------------------------------
    for row in instrument_rows:
        hist = db.load_market_data(row["symbol"], price_mode="qfq")
        if hist.empty:
            continue
        hist["time"] = pd.to_datetime(hist["time"], errors="coerce")
        hist = hist.dropna(subset=["close"]).sort_values("time")
        hist["close"] = pd.to_numeric(hist["close"], errors="coerce")
        if len(hist) < 2:
            continue
        synth_close = row["close"]
        prev_close = safe_float(hist["close"].iloc[-1], synth_close)
        if prev_close and prev_close != 0:
            row["return_1d"] = (synth_close / prev_close - 1.0) * 100.0
        for period in (5, 20, 60):
            if len(hist) > period:
                base = safe_float(hist["close"].iloc[-(period)], prev_close)
                if base and base != 0:
                    row[f"return_{period}d"] = (synth_close / base - 1.0) * 100.0

    # --- 5. Multi-level aggregation (mirrors subject_market.py) ------------
    source = pd.DataFrame(instrument_rows)
    if source.empty:
        _progress("done", 1.0, "无可用盘中数据")
        return {
            "as_of": datetime.now().isoformat(),
            "groups": [],
            "secondary_count": 0,
            "category_count": 0,
            "instrument_count": 0,
            "is_intraday": True,
            "intraday_ts": datetime.now().isoformat(),
        }

    # Filter: only instruments with full 3-level classification.
    source = source[
        (source["category_l1"].str.strip() != "")
        & (source["category_l2"].str.strip() != "")
        & (source["category_l3"].str.strip() != "")
    ].copy()

    if source.empty:
        _progress("done", 1.0, "无完整分类的标的")
        return {
            "as_of": datetime.now().isoformat(),
            "groups": [],
            "secondary_count": 0,
            "category_count": 0,
            "instrument_count": 0,
            "is_intraday": True,
            "intraday_ts": datetime.now().isoformat(),
        }

    # ---- helpers for aggregation ----
    def _metrics_summary_intra(rows_df: pd.DataFrame, meta: dict, *, is_instrument: bool = False) -> dict | None:
        if rows_df.empty:
            return None
        trend_vals: list[float] = []
        for _, r in rows_df.iterrows():
            v = _number(r.get("trend_score"))
            if v is not None:
                trend_vals.append(v)

        if not trend_vals:
            return None

        avg_trend = float(np.mean(trend_vals))
        avg_1d = float(rows_df["return_1d"].mean()) if "return_1d" in rows_df else 0.0
        avg_5d = float(rows_df["return_5d"].mean()) if "return_5d" in rows_df else 0.0
        avg_20d = float(rows_df["return_20d"].mean()) if "return_20d" in rows_df else 0.0
        avg_60d = float(rows_df["return_60d"].mean()) if "return_60d" in rows_df else 0.0
        total_amount = float(rows_df["amount"].sum()) if "amount" in rows_df else 0.0

        result = {
            "member_count": int(meta.get("member_count", len(rows_df))),
            "trend_score": avg_trend,
            "trend_ma5": avg_trend,  # simplified: single snapshot, no MA5 history
            "daily_change_pct": avg_1d,
            "change_5d": avg_5d,
            "change_20d": avg_20d,
            "change_60d": avg_60d,
            "amount": total_amount,
            "trend_history": [],
            "trend_dates": [],
            "as_of": datetime.now().date().isoformat(),
            "priority_l1": _priority(meta.get("priority_l1", 9999)),
            "priority_l2": _priority(meta.get("priority_l2", 9999)),
            "priority_l3": _priority(meta.get("priority_l3", 9999)),
            "is_intraday": True,
        }

        # Pass through trend phase for instrument-level rows only.
        if is_instrument and len(rows_df) == 1:
            row = rows_df.iloc[0]
            result["trend_phase"] = row.get("trend_phase")
            result["trend_phase_days"] = _number(row.get("trend_phase_days"))
            result["trend_phase_change_pct"] = _number(row.get("trend_phase_change_pct"))
            result["trend_phase_signal_date"] = row.get("trend_phase_signal_date")
        else:
            result["trend_phase"] = None
            result["trend_phase_days"] = None
            result["trend_phase_change_pct"] = None
            result["trend_phase_signal_date"] = None

        return result

    # L2 / L3 / instrument level aggregation.
    l2_columns = ["category_l1", "category_l2"]
    l3_columns = ["category_l1", "category_l2", "category_l3"]
    inst_columns = ["category_l1", "category_l2", "category_l3", "symbol", "name"]

    def _build_summaries(df: pd.DataFrame, group_cols: list[str], *, is_instrument: bool = False) -> list[dict]:
        summaries: list[dict] = []
        for key, grp in df.groupby(group_cols, sort=False):
            key_tuple_vals = tuple(str(v) for v in (_key_tuple(key)))
            meta_counts = {
                "member_count": grp["symbol"].nunique() if "symbol" in grp.columns else len(grp),
                "priority_l1": int(grp["priority_l1"].min()) if "priority_l1" in grp.columns else 9999,
                "priority_l2": int(grp["priority_l2"].min()) if "priority_l2" in grp.columns else 9999,
                "priority_l3": int(grp["priority_l3"].min()) if "priority_l3" in grp.columns else 9999,
            }
            summary = _metrics_summary_intra(grp, meta_counts, is_instrument=is_instrument)
            if summary:
                summary.update(dict(zip(group_cols, key_tuple_vals)))
                summaries.append(summary)
        return summaries

    l2_items = _build_summaries(source, l2_columns)
    l3_items = _build_summaries(source, l3_columns)
    instruments = _build_summaries(source, inst_columns, is_instrument=True)

    # Assign strength percentiles.
    def _assign_strength(items: list[dict], scope_cols: tuple[str, ...]) -> None:
        values_by_scope: dict[tuple[str, ...], list[float]] = defaultdict(list)
        for item in items:
            v = _number(item.get("trend_ma5"))
            if v is not None:
                values_by_scope[tuple(str(item[c]) for c in scope_cols)].append(v)
        for item in items:
            scope = tuple(str(item[c]) for c in scope_cols)
            item["strength"] = _strength(values_by_scope[scope], _number(item.get("trend_ma5")))

    _assign_strength(l2_items, ("category_l1",))
    _assign_strength(l3_items, ("category_l1",))
    _assign_strength(instruments, ("category_l1",))

    # Nest: instruments → l3 → l2.
    inst_by_l3: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for inst in instruments:
        inst_by_l3[(inst["category_l1"], inst["category_l2"], inst["category_l3"])].append(inst)

    def _sort(items: list[dict], name_key: str) -> None:
        items.sort(key=lambda x: (
            x["strength"] is None,
            -(x["strength"] or 0),
            x.get("priority_l3", 9999),
            str(x.get(name_key) or ""),
        ))

    for children in inst_by_l3.values():
        _sort(children, "name")

    l3_by_l2: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for l3 in l3_items:
        l3["children"] = inst_by_l3.get((l3["category_l1"], l3["category_l2"], l3["category_l3"]), [])
        l3["child_count"] = len(l3["children"])
        l3_by_l2[(l3["category_l1"], l3["category_l2"])].append(l3)
    for children in l3_by_l2.values():
        _sort(children, "category_l3")

    l2_by_l1: dict[str, list[dict]] = defaultdict(list)
    for l2 in l2_items:
        l2["children"] = l3_by_l2.get((l2["category_l1"], l2["category_l2"]), [])
        l2["child_count"] = len(l2["children"])
        l2_by_l1[l2["category_l1"]].append(l2)
    for children in l2_by_l1.values():
        _sort(children, "category_l2")

    groups = [
        {
            "category_l1": l1,
            "count": len(items),
            "items": items,
            "priority_l1": min(item.get("priority_l1", 9999) for item in items),
            "is_intraday": True,
        }
        for l1, items in l2_by_l1.items()
    ]
    groups.sort(key=lambda g: (g["priority_l1"], g["category_l1"]))

    _progress("done", 1.0, f"完成 — {len(groups)} 个一级类目, {len(instruments)} 个标的")

    return {
        "as_of": datetime.now().isoformat(),
        "groups": groups,
        "secondary_count": len(l2_items),
        "category_count": len(l3_items),
        "instrument_count": len(instruments),
        "is_intraday": True,
        "intraday_ts": datetime.now().isoformat(),
    }
