from __future__ import annotations

from datetime import date

import pandas as pd


def equal_weight_pool_benchmark(
    market_data: dict[str, pd.DataFrame],
    timeline: list[date],
    initial_capital: float,
    lot_size: int = 100,
) -> dict:
    if not timeline:
        return {"name": "equal_weight_pool", "series": []}

    symbols = sorted(market_data.keys())
    if not symbols:
        return {"name": "equal_weight_pool", "series": []}

    per_symbol_capital = initial_capital / len(symbols)
    states = {
        symbol: {"cash": per_symbol_capital, "qty": 0, "last_close": 0.0}
        for symbol in symbols
    }

    series: list[dict] = []
    for day in timeline:
        day_value = 0.0
        for symbol in symbols:
            df = market_data[symbol]
            row = df[df["date"] == day]
            if row.empty:
                close = states[symbol]["last_close"]
            else:
                close = float(row.iloc[-1]["close"])
                states[symbol]["last_close"] = close

            if states[symbol]["qty"] == 0 and close > 0:
                qty = int((states[symbol]["cash"] // close) // lot_size) * lot_size
                if qty > 0:
                    states[symbol]["qty"] = qty
                    states[symbol]["cash"] -= qty * close

            day_value += states[symbol]["cash"] + states[symbol]["qty"] * close

        series.append({"date": day.isoformat(), "equity": day_value})

    return {"name": "equal_weight_pool", "series": series}


def single_symbol_benchmark(
    benchmark_data: pd.DataFrame,
    timeline: list[date],
    initial_capital: float,
    lot_size: int = 100,
    symbol: str = "",
) -> dict:
    benchmark_symbol = str(symbol or "").strip().upper()
    name = f"symbol:{benchmark_symbol}" if benchmark_symbol else "symbol"
    if not timeline:
        return {"name": name, "series": []}
    if benchmark_data.empty:
        return {"name": name, "series": []}

    state = {"cash": float(initial_capital), "qty": 0, "last_close": 0.0}
    series: list[dict] = []

    for day in timeline:
        row = benchmark_data[benchmark_data["date"] == day]
        if row.empty:
            close = float(state["last_close"])
        else:
            close = float(row.iloc[-1]["close"])
            state["last_close"] = close

        if state["qty"] == 0 and close > 0:
            qty = int((state["cash"] // close) // lot_size) * lot_size
            if qty > 0:
                state["qty"] = qty
                state["cash"] -= qty * close

        day_value = state["cash"] + state["qty"] * close
        series.append({"date": day.isoformat(), "equity": day_value})

    return {"name": name, "series": series}
