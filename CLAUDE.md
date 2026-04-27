# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A-share ETF trend trading system (single-user, local run, manual trade execution). Built with FastAPI + Jinja2 frontend, APScheduler for intraday polling, and SQLite as the primary store.

## Common Commands

- **Run dev server (Windows):** `powershell -ExecutionPolicy Bypass -File .\scripts\run_dev.ps1`
- **Run dev server (manual):** `$env:PYTHONPATH = "src"; uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload`
- **Install dependencies:** `.\venv\Scripts\python.exe -m pip install -e .`
- **Lint:** `ruff check src` (line-length 100, configured in `pyproject.toml`)
- **No project tests exist.** The repo has no test suite.

## High-Level Architecture

### Application Lifecycle

`src/app/main.py` bootstraps the app via FastAPI `lifespan`:
1. Calls `init_db()` to initialize the SQLite singleton.
2. Instantiates `SignalEngine` with the configured provider priority.
3. Registers three scheduler jobs in `SchedulerManager`: intraday poll, 14:45 final signal (with retry), and 15:30 daily market data update.
4. Stores `signal_engine`, `scheduler_manager`, and `settings` on `app.state`.

### Storage Model

All persistent state lives in `data/trend_quant.db` (SQLite). Access is through a global singleton (`data/storage/db.py`):
- `init_db()` must be called before `get_db()`.
- Tables: `market_data`, `signals`, `signal_states`, `manual_trades`, `position_snapshots`, `backtests`, `optimization_jobs`.
- `RuntimeStore` (`data/storage/runtime_store.py`) still handles a few JSON/JSONL files (advice, calc logs) under `data/runtime/` and `logs/`.

### Data Provider Chain

`DataService` (`src/data/service.py`) maintains a priority-ordered provider list (default: `efinance`, then `akshare`). Each provider implements `IDataProvider` and returns normalized OHLCV DataFrames. If the primary fails, the next is tried automatically. `SignalEngine` and backtests consume data through `MarketStore`, which reads/writes the SQLite `market_data` table.

### Signal Engine Flow (`src/engine/signal_engine.py`)

`run_poll(trigger_name)` executes per trading day:
1. Loads `config/instruments.yaml` and `config/strategy.yaml`.
2. For each enabled symbol, ensures local bars exist in SQLite; if stale, fetches daily history via `DataService`.
3. Fuses the latest quote into the most recent bar, **but skips quotes with >30% price spikes** to avoid corrupting signal state.
4. Derives stop prices (hard stop and chandelier stop) from position state + ATR.
5. Evaluates each symbol via `TrendScoreStrategy`.
6. Computes risk-budget position sizing via `RiskSizer`, caps per-position cost, then scales all BUY candidates proportionally if total cash is exceeded.
7. Saves the full signal payload to `signals` table and updates `signal_states` table.

### Strategy Catalog & Config Resolution (`src/strategy/catalog.py`)

The system supports four strategy IDs:
- `trend_score_v1` — single-asset trend score with stop rules
- `momentum_topn_v1` / `v2` / `v3` — TopN momentum strategies with weekly rebalance and configurable buy filters / sell signals

`resolve_strategy_config()` layers defaults, `strategy.yaml` sub-keys (`momentum_topn`, `momentum_topn_v2`, `momentum_topn_v3`), and any runtime overrides. `BacktestEngine` selects the implementation from a strategy map by ID.

### Backtest Engine (`src/backtest/backtest_engine.py`)

Day-by-day simulation across a merged timeline of all selected symbols:
- **Execution order:** sells first, then buys.
- **Trend mode:** per-symbol signals, individual position sizing with max-position cap and cash scaling.
- **Momentum mode:** `finalize_day()` produces a rebalance plan (TopN ranked by hybrid score). Buys are filled in rank order and cash-scaled.
- **Sell reference price:** hard-stop or chandelier-stop triggers use the stop price as execution reference; otherwise close price.
- **Costs:** fee rate + minimum fee + slippage applied on every trade.

### Portfolio Reconstruction (`src/portfolio/service.py`)

`PortfolioService` does **not** track live positions. It rebuilds them by replaying all rows from the `manual_trades` table in chronological order:
- BUY adds a lot.
- SELL consumes lots FIFO.
- `sellable_qty` respects T+1 by counting only lots with `buy_date < as_of_date`.
- The resulting snapshot is saved to `position_snapshots`.

### Risk Sizing (`src/portfolio/risk_sizer.py`)

- `suggest_qty`: `risk_budget / (ATR * stop_mul)`, rounded down to whole lots (lot size = 100).
- `cap_qty_by_max_cost`: caps quantity so estimated cost does not exceed a per-position maximum.
- `scale_allocations`: when total candidate cost exceeds available cash, all BUY quantities are scaled by the same ratio so the portfolio stays within budget.

## Important File Map

- `src/app/main.py` — FastAPI entry, lifespan, scheduler wiring
- `src/engine/signal_engine.py` — intraday poll and daily update orchestration
- `src/backtest/backtest_engine.py` — backtest simulation engine
- `src/strategy/catalog.py` — strategy registry and config resolution
- `src/strategy/trend_score_strategy.py` — primary strategy implementation
- `src/data/storage/db.py` — SQLite schema and all DB operations
- `src/data/service.py` — data provider orchestration
- `src/portfolio/service.py` — trade replay and position snapshot
- `src/portfolio/risk_sizer.py` — position sizing logic
- `config/app.yaml` — runtime settings (host, port, polling times, provider priority, lot size)
- `config/instruments.yaml` — ETF pool and per-symbol risk params
- `config/strategy.yaml` — strategy parameters including momentum sub-configs
