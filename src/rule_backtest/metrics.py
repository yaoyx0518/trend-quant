from __future__ import annotations

import numpy as np
import pandas as pd


def compute_drawdown(daily_nav: list[dict]) -> list[dict]:
    if not daily_nav:
        return []
    df = pd.DataFrame(daily_nav)
    equity = pd.to_numeric(df["equity"], errors="coerce")
    rolling_max = equity.cummax().replace(0, np.nan)
    dd = (equity / rolling_max - 1.0).fillna(0.0)
    return [{"date": str(row["date"]), "drawdown": float(dd.iloc[idx])} for idx, row in df.iterrows()]


def compute_summary(daily_nav: list[dict], trades: list[dict], turnover_total: float) -> dict:
    if not daily_nav:
        return {
            "total_return": 0.0,
            "annual_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "calmar": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "trade_count": 0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "payoff_ratio": 0.0,
            "turnover": 0.0,
            "total_commission": 0.0,
            "total_stamp_tax": 0.0,
            "total_trading_cost": 0.0,
        }

    df = pd.DataFrame(daily_nav)
    df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
    df = df.dropna(subset=["equity"])
    start_equity = float(df["equity"].iloc[0])
    end_equity = float(df["equity"].iloc[-1])
    total_return = end_equity / start_equity - 1.0 if start_equity else 0.0
    n_days = max(len(df) - 1, 1)
    annual_return = (1.0 + total_return) ** (252.0 / n_days) - 1.0 if total_return > -1 else -1.0

    returns = df["equity"].pct_change().dropna()
    mean_ret = float(returns.mean()) if not returns.empty else 0.0
    std_ret = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
    sharpe = mean_ret / std_ret * np.sqrt(252.0) if std_ret > 0 else 0.0
    downside = returns[returns < 0]
    downside_std = float(downside.std(ddof=1)) if len(downside) > 1 else 0.0
    sortino = mean_ret / downside_std * np.sqrt(252.0) if downside_std > 0 else 0.0

    dd_rows = compute_drawdown(daily_nav)
    max_drawdown = min((float(row["drawdown"]) for row in dd_rows), default=0.0)
    calmar = annual_return / abs(max_drawdown) if max_drawdown < 0 else 0.0

    sell_pnls = [float(t.get("pnl", 0.0) or 0.0) for t in trades if str(t.get("side", "")).upper() == "SELL"]
    wins = [x for x in sell_pnls if x > 0]
    losses = [x for x in sell_pnls if x < 0]
    win_rate = len(wins) / len(sell_pnls) if sell_pnls else 0.0
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
    avg_win = gross_profit / len(wins) if wins else 0.0
    avg_loss = gross_loss / len(losses) if losses else 0.0
    payoff_ratio = avg_win / avg_loss if avg_loss > 0 else 0.0
    avg_equity = float(df["equity"].mean()) if not df.empty else 0.0
    turnover = turnover_total / avg_equity if avg_equity > 0 else 0.0
    total_commission = sum(float(t.get("commission", 0.0) or 0.0) for t in trades)
    total_stamp_tax = sum(float(t.get("stamp_tax", 0.0) or 0.0) for t in trades)

    return {
        "total_return": float(total_return),
        "annual_return": float(annual_return),
        "max_drawdown": float(max_drawdown),
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "calmar": float(calmar),
        "win_rate": float(win_rate),
        "profit_factor": float(profit_factor),
        "trade_count": int(len(trades)),
        "closed_trade_count": int(len(sell_pnls)),
        "avg_win": float(avg_win),
        "avg_loss": float(avg_loss),
        "payoff_ratio": float(payoff_ratio),
        "turnover": float(turnover),
        "total_commission": float(total_commission),
        "total_stamp_tax": float(total_stamp_tax),
        "total_trading_cost": float(total_commission + total_stamp_tax),
    }


def annual_returns(daily_nav: list[dict]) -> list[dict]:
    if not daily_nav:
        return []
    df = pd.DataFrame(daily_nav)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
    df = df.dropna(subset=["date", "equity"]).sort_values("date")
    if df.empty:
        return []
    df["year"] = df["date"].dt.year
    year_end = df.groupby("year", as_index=False).last()[["year", "equity"]]
    rows: list[dict] = []
    prev = float(df["equity"].iloc[0])
    for _, row in year_end.iterrows():
        equity = float(row["equity"])
        rows.append({"year": int(row["year"]), "return": equity / prev - 1.0 if prev else 0.0})
        prev = equity
    return rows


def monthly_returns(daily_nav: list[dict]) -> list[dict]:
    if not daily_nav:
        return []
    df = pd.DataFrame(daily_nav)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
    df = df.dropna(subset=["date", "equity"]).sort_values("date")
    if df.empty:
        return []
    monthly = df.set_index("date")["equity"].resample("ME").last().dropna()
    returns = monthly.pct_change().dropna()
    return [{"month": ts.strftime("%Y-%m"), "return": float(value)} for ts, value in returns.items()]
