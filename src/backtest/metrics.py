from __future__ import annotations

from collections import defaultdict

import numpy as np
import pandas as pd


def _to_nav_df(daily_nav: list[dict]) -> pd.DataFrame:
    if not daily_nav:
        return pd.DataFrame(columns=["date", "equity"])
    df = pd.DataFrame(daily_nav)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "equity"]).sort_values("date").reset_index(drop=True)
    df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
    df = df.dropna(subset=["equity"])
    return df


def compute_drawdown(daily_nav: list[dict]) -> list[float]:
    df = _to_nav_df(daily_nav)
    if df.empty:
        return []
    rolling_max = df["equity"].cummax().replace(0, np.nan)
    drawdown = (df["equity"] / rolling_max - 1.0).fillna(0.0)
    return drawdown.tolist()


def _collect_sell_pnls(trades: list[dict]) -> list[float]:
    return [float(t.get("pnl", 0.0) or 0.0) for t in trades if str(t.get("side", "")).upper() == "SELL"]


def _compute_trade_win_rate(trades: list[dict]) -> tuple[float, int]:
    pnl_rows = _collect_sell_pnls(trades)
    if not pnl_rows:
        return 0.0, 0
    wins = sum(1 for x in pnl_rows if x > 0)
    total = len(pnl_rows)
    return wins / total, total


def _profit_factor_from_pnls(pnl_rows: list[float]) -> float:
    if not pnl_rows:
        return 0.0
    gain = sum(x for x in pnl_rows if x > 0)
    loss = abs(sum(x for x in pnl_rows if x < 0))
    if loss <= 0:
        return 999.0 if gain > 0 else 0.0
    return gain / loss


def _compute_profit_factor(trades: list[dict]) -> float:
    return _profit_factor_from_pnls(_collect_sell_pnls(trades))


def _annual_return_rows(daily_nav: list[dict]) -> list[dict]:
    df = _to_nav_df(daily_nav)
    if df.empty:
        return []

    start_equity = float(df["equity"].iloc[0])
    df["year"] = df["date"].dt.year
    year_end = df.groupby("year", as_index=False).last()[["year", "equity"]]

    rows: list[dict] = []
    prev = start_equity
    for _, row in year_end.iterrows():
        year = int(row["year"])
        equity = float(row["equity"])
        ret = equity / prev - 1.0 if prev else 0.0
        rows.append({"year": year, "return": ret})
        prev = equity
    return rows


def _annual_sharpe_map(daily_nav: list[dict]) -> dict[int, float]:
    df = _to_nav_df(daily_nav)
    if df.empty:
        return {}
    df["year"] = df["date"].dt.year

    out: dict[int, float] = {}
    for year, group in df.groupby("year"):
        returns = group["equity"].pct_change().dropna()
        mean_ret = float(returns.mean()) if not returns.empty else 0.0
        std_ret = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
        sharpe = (mean_ret / std_ret * np.sqrt(252.0)) if std_ret > 0 else 0.0
        out[int(year)] = float(sharpe)
    return out


def _parse_trade_year(value: object) -> int | None:
    text = str(value or "").strip()
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return None
    return int(dt.year)


def _annual_trade_stats_map(trades: list[dict]) -> dict[int, dict]:
    pnl_map: dict[int, list[float]] = defaultdict(list)
    for t in trades or []:
        if str(t.get("side", "")).upper() != "SELL":
            continue
        year = _parse_trade_year(t.get("date"))
        if year is None:
            continue
        pnl_map[year].append(float(t.get("pnl", 0.0) or 0.0))

    out: dict[int, dict] = {}
    for year, pnl_rows in pnl_map.items():
        trade_count = len(pnl_rows)
        win_rate = (sum(1 for x in pnl_rows if x > 0) / trade_count) if trade_count > 0 else 0.0
        profit_factor = _profit_factor_from_pnls(pnl_rows)
        out[int(year)] = {
            "trade_count": int(trade_count),
            "win_rate": float(win_rate),
            "profit_factor": float(profit_factor),
        }
    return out


def compute_metrics(daily_nav: list[dict], trades: list[dict], turnover_total: float) -> dict:
    df = _to_nav_df(daily_nav)
    if df.empty:
        return {
            "total_return": 0.0,
            "annual_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe": 0.0,
            "win_rate": 0.0,
            "calmar": 0.0,
            "sortino": 0.0,
            "turnover": 0.0,
            "trade_count": 0,
            "profit_factor": 0.0,
        }

    returns = df["equity"].pct_change().dropna()
    total_return = float(df["equity"].iloc[-1] / df["equity"].iloc[0] - 1.0)

    n_days = max(len(df) - 1, 1)
    annual_factor = 252.0 / n_days
    annual_return = float((1.0 + total_return) ** annual_factor - 1.0) if (1.0 + total_return) > 0 else -1.0

    drawdown_series = pd.Series(compute_drawdown(daily_nav))
    max_drawdown = float(drawdown_series.min()) if not drawdown_series.empty else 0.0

    mean_ret = float(returns.mean()) if not returns.empty else 0.0
    std_ret = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
    sharpe = (mean_ret / std_ret * np.sqrt(252.0)) if std_ret > 0 else 0.0

    downside = returns[returns < 0]
    downside_std = float(downside.std(ddof=1)) if len(downside) > 1 else 0.0
    sortino = (mean_ret / downside_std * np.sqrt(252.0)) if downside_std > 0 else 0.0

    calmar = annual_return / abs(max_drawdown) if max_drawdown < 0 else 0.0

    avg_equity = float(df["equity"].mean()) if not df.empty else 0.0
    turnover = (turnover_total / avg_equity) if avg_equity > 0 else 0.0

    win_rate, _closed_count = _compute_trade_win_rate(trades)
    profit_factor = _compute_profit_factor(trades)

    return {
        "total_return": total_return,
        "annual_return": annual_return,
        "max_drawdown": max_drawdown,
        "sharpe": float(sharpe),
        "win_rate": float(win_rate),
        "calmar": float(calmar),
        "sortino": float(sortino),
        "turnover": float(turnover),
        "trade_count": len(trades),
        "profit_factor": float(profit_factor),
    }


def compute_annual_returns(
    daily_nav: list[dict],
    trades: list[dict] | None = None,
    benchmark_daily_nav: list[dict] | None = None,
) -> list[dict]:
    strategy_rows = _annual_return_rows(daily_nav)
    if not strategy_rows:
        return []

    strategy_sharpe = _annual_sharpe_map(daily_nav)
    trade_stats = _annual_trade_stats_map(trades or [])
    benchmark_rows = _annual_return_rows(benchmark_daily_nav or [])
    benchmark_return_map = {int(r["year"]): float(r["return"]) for r in benchmark_rows}
    benchmark_sharpe_map = _annual_sharpe_map(benchmark_daily_nav or [])

    out: list[dict] = []
    for row in strategy_rows:
        year = int(row["year"])
        tstats = trade_stats.get(year, {})
        out.append(
            {
                "year": year,
                "return": float(row["return"]),
                "trade_count": int(tstats.get("trade_count", 0)),
                "win_rate": float(tstats.get("win_rate", 0.0)),
                "profit_factor": float(tstats.get("profit_factor", 0.0)),
                "sharpe": float(strategy_sharpe.get(year, 0.0)),
                "benchmark_return": benchmark_return_map.get(year),
                "benchmark_sharpe": benchmark_sharpe_map.get(year),
            }
        )
    return out


def compute_monthly_heatmap(daily_nav: list[dict]) -> dict:
    df = _to_nav_df(daily_nav)
    if df.empty:
        return {"years": [], "months": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"], "data": []}

    monthly = df.set_index("date")["equity"].resample("ME").last().dropna()
    monthly_ret = monthly.pct_change().dropna()

    years = sorted(monthly_ret.index.year.unique().tolist())
    year_idx = {y: i for i, y in enumerate(years)}

    data: list[list[float]] = []
    for ts, ret in monthly_ret.items():
        y = int(ts.year)
        m = int(ts.month) - 1
        data.append([m, year_idx[y], float(ret * 100.0)])

    return {
        "years": years,
        "months": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
        "data": data,
    }


def compute_symbol_trade_stats(trades: list[dict], symbols: list[str] | None = None) -> list[dict]:
    pnl_map: dict[str, list[float]] = defaultdict(list)

    known_symbols = [str(s) for s in (symbols or []) if str(s).strip()]
    for symbol in known_symbols:
        pnl_map[symbol] = []

    for t in trades:
        if str(t.get("side", "")).upper() != "SELL":
            continue
        symbol = str(t.get("symbol", "")).strip()
        if not symbol:
            continue
        pnl_map[symbol].append(float(t.get("pnl", 0.0) or 0.0))

    rows: list[dict] = []
    for symbol in sorted(pnl_map.keys()):
        pnls = pnl_map[symbol]
        trade_count = len(pnls)
        win_rate = (sum(1 for x in pnls if x > 0) / trade_count) if trade_count > 0 else 0.0
        profit_factor = _profit_factor_from_pnls(pnls)
        contribution = float(sum(pnls))
        rows.append(
            {
                "symbol": symbol,
                "trade_count": trade_count,
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "contribution": contribution,
            }
        )

    return rows
