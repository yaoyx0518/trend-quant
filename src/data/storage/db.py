from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

_db_instance: Database | None = None


class Database:
    def __init__(self, db_path: str | Path = "data/trend_quant.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_tables()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_tables(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS manual_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty INTEGER,
                    price REAL,
                    fee REAL,
                    trade_time TEXT,
                    note TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_trades_date ON manual_trades(trade_date);
                CREATE INDEX IF NOT EXISTS idx_trades_symbol ON manual_trades(symbol);

                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_day TEXT NOT NULL UNIQUE,
                    ts TEXT,
                    trigger TEXT,
                    status TEXT,
                    payload TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_signals_day ON signals(trade_day);

                CREATE TABLE IF NOT EXISTS signal_states (
                    symbol TEXT PRIMARY KEY,
                    trend_score REAL,
                    prev_trend_score REAL,
                    position_qty INTEGER,
                    updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS position_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    as_of_date TEXT,
                    cash REAL,
                    positions TEXT,
                    trade_count INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS backtests (
                    run_id TEXT PRIMARY KEY,
                    status TEXT,
                    strategy_id TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    total_return REAL,
                    win_rate REAL,
                    profit_factor REAL,
                    sharpe REAL,
                    trade_count INTEGER,
                    timeline_days INTEGER,
                    summary TEXT,
                    meta TEXT,
                    input TEXT,
                    result_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_backtests_created ON backtests(created_at);

                CREATE TABLE IF NOT EXISTS optimization_jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT,
                    progress TEXT,
                    summary TEXT,
                    current TEXT,
                    result TEXT,
                    created_at TEXT,
                    finished_at TEXT
                );

                CREATE TABLE IF NOT EXISTS market_data (
                    symbol TEXT NOT NULL,
                    time TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    amount REAL,
                    provider TEXT,
                    PRIMARY KEY (symbol, time)
                );
                CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time ON market_data(symbol, time);
                """
            )

    # ------------------------------------------------------------------
    # manual_trades
    # ------------------------------------------------------------------
    def add_trade(self, trade: dict[str, Any]) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """INSERT INTO manual_trades
                   (trade_date, symbol, side, qty, price, fee, trade_time, note)
                   VALUES (:trade_date, :symbol, :side, :qty, :price, :fee, :trade_time, :note)""",
                trade,
            )
            return cursor.lastrowid or 0

    def get_trades_by_date(self, trade_date: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM manual_trades WHERE trade_date = ? ORDER BY trade_time",
                (trade_date,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_all_trades(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM manual_trades ORDER BY trade_date, trade_time"
            ).fetchall()
            return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # signals
    # ------------------------------------------------------------------
    def save_signals(self, trade_day: str, payload: dict) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO signals (trade_day, ts, trigger, status, payload)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(trade_day) DO UPDATE SET
                     ts=excluded.ts,
                     trigger=excluded.trigger,
                     status=excluded.status,
                     payload=excluded.payload""",
                (
                    trade_day,
                    payload.get("ts"),
                    payload.get("trigger"),
                    payload.get("status"),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )

    def get_signals(self, trade_day: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload FROM signals WHERE trade_day = ?", (trade_day,)
            ).fetchone()
            return json.loads(row["payload"]) if row else None

    def get_latest_signals(self, limit: int = 1) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT payload FROM signals ORDER BY trade_day DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [json.loads(row["payload"]) for row in rows]

    def list_signal_days(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT trade_day FROM signals ORDER BY trade_day DESC"
            ).fetchall()
            return [row["trade_day"] for row in rows]

    # ------------------------------------------------------------------
    # signal_states
    # ------------------------------------------------------------------
    def save_signal_state(self, states: dict[str, dict]) -> None:
        with self._connect() as conn:
            for symbol, state in states.items():
                conn.execute(
                    """INSERT INTO signal_states
                       (symbol, trend_score, prev_trend_score, position_qty, updated_at)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(symbol) DO UPDATE SET
                         trend_score=excluded.trend_score,
                         prev_trend_score=excluded.prev_trend_score,
                         position_qty=excluded.position_qty,
                         updated_at=excluded.updated_at""",
                    (
                        symbol,
                        state.get("trend_score"),
                        state.get("prev_trend_score"),
                        state.get("position_qty"),
                        state.get("updated_at"),
                    ),
                )

    def get_signal_state(self, symbol: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM signal_states WHERE symbol = ?", (symbol,)
            ).fetchone()
            return dict(row) if row else None

    def get_all_signal_states(self) -> dict[str, dict]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM signal_states").fetchall()
            return {row["symbol"]: dict(row) for row in rows}

    # ------------------------------------------------------------------
    # position_snapshots
    # ------------------------------------------------------------------
    def save_position_snapshot(self, snapshot: dict) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO position_snapshots (as_of_date, cash, positions, trade_count)
                   VALUES (?, ?, ?, ?)""",
                (
                    snapshot.get("as_of_date"),
                    snapshot.get("cash"),
                    json.dumps(snapshot.get("positions", {}), ensure_ascii=False),
                    snapshot.get("trade_count"),
                ),
            )

    def get_latest_position_snapshot(self) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM position_snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if not row:
                return None
            d = dict(row)
            d["positions"] = json.loads(d["positions"]) if d.get("positions") else {}
            return d

    # ------------------------------------------------------------------
    # backtests
    # ------------------------------------------------------------------
    def save_backtest(self, run_id: str, result: dict) -> None:
        status = result.get("status")
        inp = result.get("input", {}) if isinstance(result.get("input"), dict) else {}
        meta = result.get("meta", {}) if isinstance(result.get("meta"), dict) else {}
        summary = result.get("summary", {}) if isinstance(result.get("summary"), dict) else {}

        with self._connect() as conn:
            conn.execute(
                """INSERT INTO backtests
                   (run_id, status, strategy_id, start_date, end_date,
                    total_return, win_rate, profit_factor, sharpe, trade_count,
                    timeline_days, summary, meta, input, result_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(run_id) DO UPDATE SET
                     status=excluded.status,
                     strategy_id=excluded.strategy_id,
                     start_date=excluded.start_date,
                     end_date=excluded.end_date,
                     total_return=excluded.total_return,
                     win_rate=excluded.win_rate,
                     profit_factor=excluded.profit_factor,
                     sharpe=excluded.sharpe,
                     trade_count=excluded.trade_count,
                     timeline_days=excluded.timeline_days,
                     summary=excluded.summary,
                     meta=excluded.meta,
                     input=excluded.input,
                     result_json=excluded.result_json""",
                (
                    run_id,
                    status,
                    meta.get("strategy_id") or inp.get("strategy_id"),
                    inp.get("start_date"),
                    inp.get("end_date"),
                    summary.get("total_return"),
                    summary.get("win_rate"),
                    summary.get("profit_factor"),
                    summary.get("sharpe"),
                    summary.get("trade_count"),
                    meta.get("timeline_days"),
                    json.dumps(summary, ensure_ascii=False),
                    json.dumps(meta, ensure_ascii=False),
                    json.dumps(inp, ensure_ascii=False),
                    json.dumps(result, ensure_ascii=False),
                ),
            )

    def get_backtest(self, run_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT result_json FROM backtests WHERE run_id = ?", (run_id,)
            ).fetchone()
            return json.loads(row["result_json"]) if row else None

    def list_backtests(self, limit: int = 40) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT run_id, status, strategy_id, start_date, end_date,
                          total_return, win_rate, profit_factor, sharpe,
                          trade_count, timeline_days, summary, meta, input
                   FROM backtests
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()

        items: list[dict] = []
        for row in rows:
            d = dict(row)
            summary = json.loads(d["summary"]) if d.get("summary") else {}
            meta = json.loads(d["meta"]) if d.get("meta") else {}
            inp = json.loads(d["input"]) if d.get("input") else {}
            strategy_overrides = inp.get("strategy_overrides")
            strategy_params = inp.get("strategy_params")
            params = (
                strategy_overrides
                if isinstance(strategy_overrides, dict)
                else (strategy_params if isinstance(strategy_params, dict) else {})
            )
            items.append(
                {
                    "run_id": d["run_id"],
                    "status": d["status"],
                    "strategy": meta.get("strategy_id") or inp.get("strategy_id"),
                    "start_date": inp.get("start_date"),
                    "end_date": inp.get("end_date"),
                    "params": params,
                    "total_return": summary.get("total_return") if d["status"] == "ok" else None,
                    "win_rate": summary.get("win_rate") if d["status"] == "ok" else None,
                    "profit_factor": summary.get("profit_factor") if d["status"] == "ok" else None,
                    "sharpe": summary.get("sharpe") if d["status"] == "ok" else None,
                    "trade_count": summary.get("trade_count") if d["status"] == "ok" else None,
                    "timeline_days": meta.get("timeline_days"),
                }
            )
        return items

    def list_backtests_summary(self, limit: int = 40) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT run_id, status, start_date, end_date, total_return
                   FROM backtests
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # optimization_jobs
    # ------------------------------------------------------------------
    def save_optimization_job(
        self,
        job_id: str,
        status: dict,
        result: dict | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO optimization_jobs
                   (job_id, status, progress, summary, current, result, created_at, finished_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(job_id) DO UPDATE SET
                     status=excluded.status,
                     progress=excluded.progress,
                     summary=excluded.summary,
                     current=excluded.current,
                     result=COALESCE(excluded.result, optimization_jobs.result),
                     finished_at=excluded.finished_at""",
                (
                    job_id,
                    status.get("status"),
                    json.dumps(status.get("progress", {}), ensure_ascii=False),
                    json.dumps(status.get("summary", {}), ensure_ascii=False),
                    json.dumps(status.get("current", {}), ensure_ascii=False),
                    json.dumps(result, ensure_ascii=False) if result else None,
                    status.get("created_at"),
                    status.get("finished_at"),
                ),
            )

    def get_optimization_job(self, job_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM optimization_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if not row:
                return None
            d = dict(row)
            merged: dict = {
                "job_id": d["job_id"],
                "status": d["status"],
                "created_at": d.get("created_at"),
                "finished_at": d.get("finished_at"),
                "progress": json.loads(d["progress"]) if d.get("progress") else {},
                "summary": json.loads(d["summary"]) if d.get("summary") else {},
                "current": json.loads(d["current"]) if d.get("current") else {},
            }
            if d.get("result"):
                result = json.loads(d["result"])
                merged.update(result)
            return merged

    def get_optimization_status(self, job_id: str) -> dict | None:
        job = self.get_optimization_job(job_id)
        if job is None:
            return None
        return {
            "job_id": job["job_id"],
            "status": job["status"],
            "created_at": job.get("created_at"),
            "finished_at": job.get("finished_at"),
            "progress": job.get("progress", {}),
            "summary": job.get("summary", {}),
            "current": job.get("current", {}),
        }

    def get_optimization_result(self, job_id: str) -> dict | None:
        job = self.get_optimization_job(job_id)
        if job is None:
            return None
        return {
            "job_id": job["job_id"],
            "status": job["status"],
            "created_at": job.get("created_at"),
            "finished_at": job.get("finished_at"),
            "progress": job.get("progress", {}),
            "summary": job.get("summary", {}),
            "best": job.get("best"),
            "rows": job.get("rows", []),
            "input": job.get("input"),
            "error": job.get("error"),
        }

    # ------------------------------------------------------------------
    # Migration helpers
    # ------------------------------------------------------------------
    def migrate_manual_trades_from_json(self, runtime_store) -> int:
        from pathlib import Path

        trade_dir = Path(runtime_store.base_dir) / "trades"
        if not trade_dir.exists():
            return 0

        count = 0
        for file_path in sorted(trade_dir.glob("manual_trades_*.json")):
            payload = runtime_store.read_json(
                str(Path("trades") / file_path.name), default={"items": []}
            )
            items = payload.get("items", []) if isinstance(payload, dict) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                self.add_trade(item)
                count += 1
        return count

    def migrate_signals_from_json(self, runtime_store) -> int:
        from pathlib import Path

        signal_dir = Path(runtime_store.base_dir) / "signals"
        if not signal_dir.exists():
            return 0

        count = 0
        for file_path in sorted(signal_dir.glob("*.json")):
            if file_path.name == "latest_state.json":
                continue
            payload = runtime_store.read_json(
                str(Path("signals") / file_path.name), default={}
            )
            if not isinstance(payload, dict):
                continue
            trade_day = file_path.stem
            self.save_signals(trade_day, payload)
            count += 1
        return count

    def migrate_signal_states_from_json(self, runtime_store) -> int:
        payload = runtime_store.read_json("signals/latest_state.json", default={})
        if not isinstance(payload, dict):
            return 0
        self.save_signal_state(payload)
        return len(payload)

    def migrate_position_snapshots_from_json(self, runtime_store) -> int:
        payload = runtime_store.read_json("positions/current_positions.json", default={})
        if not isinstance(payload, dict):
            return 0
        self.save_position_snapshot(payload)
        return 1

    def migrate_backtests_from_json(self, runtime_store) -> int:
        from pathlib import Path

        base = Path(runtime_store.base_dir) / "backtests"
        if not base.exists():
            return 0

        count = 0
        for result_path in base.glob("*/result.json"):
            run_id = result_path.parent.name
            result = runtime_store.read_json(
                str(Path("backtests") / run_id / "result.json"), default={}
            )
            if not isinstance(result, dict):
                continue
            self.save_backtest(run_id, result)
            count += 1
        return count

    def migrate_optimizations_from_json(self, runtime_store) -> int:
        from pathlib import Path

        base = Path(runtime_store.base_dir) / "optimizations"
        if not base.exists():
            return 0

        count = 0
        for job_dir in base.iterdir():
            if not job_dir.is_dir():
                continue
            job_id = job_dir.name
            status = runtime_store.read_json(
                str(Path("optimizations") / job_id / "status.json"), default=None
            )
            result = runtime_store.read_json(
                str(Path("optimizations") / job_id / "result.json"), default=None
            )
            if status is None:
                continue
            self.save_optimization_job(job_id, status, result)
            count += 1
        return count

    # ------------------------------------------------------------------
    # market_data
    # ------------------------------------------------------------------
    def save_market_data(self, symbol: str, df) -> None:
        if df.empty:
            return
        records: list[tuple] = []
        for _, row in df.iterrows():
            records.append(
                (
                    symbol,
                    str(row.get("time", "")),
                    float(row["open"]) if hasattr(row, "__getitem__") and row.get("open") is not None and str(row.get("open")) != "nan" else None,
                    float(row["high"]) if hasattr(row, "__getitem__") and row.get("high") is not None and str(row.get("high")) != "nan" else None,
                    float(row["low"]) if hasattr(row, "__getitem__") and row.get("low") is not None and str(row.get("low")) != "nan" else None,
                    float(row["close"]) if hasattr(row, "__getitem__") and row.get("close") is not None and str(row.get("close")) != "nan" else None,
                    float(row["volume"]) if hasattr(row, "__getitem__") and row.get("volume") is not None and str(row.get("volume")) != "nan" else None,
                    float(row["amount"]) if hasattr(row, "__getitem__") and row.get("amount") is not None and str(row.get("amount")) != "nan" else None,
                    str(row.get("provider", "")) if hasattr(row, "__getitem__") and row.get("provider") is not None else None,
                )
            )
        with self._connect() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO market_data
                   (symbol, time, open, high, low, close, volume, amount, provider)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                records,
            )

    def load_market_data(self, symbol: str):
        import pandas as pd

        with self._connect() as conn:
            rows = conn.execute(
                """SELECT time, open, high, low, close, volume, amount, symbol, provider
                   FROM market_data WHERE symbol = ? ORDER BY time""",
                (symbol,),
            ).fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame([dict(r) for r in rows])
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        for col in ("open", "high", "low", "close", "volume", "amount"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def list_market_symbols(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM market_data ORDER BY symbol"
            ).fetchall()
            return [r["symbol"] for r in rows]

    def get_market_data_summary(self, symbol: str) -> dict:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT COUNT(*) AS rows, MIN(time) AS start, MAX(time) AS end
                   FROM market_data WHERE symbol = ?""",
                (symbol,),
            ).fetchone()
        if row is None or row["rows"] == 0:
            return {"rows": 0, "start": None, "end": None}
        return {"rows": row["rows"], "start": row["start"], "end": row["end"]}

    def migrate_market_data_from_parquet(self, base_dir: str = "data/market/etf") -> int:
        import pandas as pd
        from pathlib import Path

        count = 0
        for p in Path(base_dir).glob("*.parquet"):
            df = pd.read_parquet(p)
            if not df.empty:
                self.save_market_data(p.stem, df)
                count += len(df)
        return count


def init_db(db_path: str | Path = "data/trend_quant.db") -> Database:
    global _db_instance
    _db_instance = Database(db_path)
    return _db_instance


def get_db() -> Database:
    if _db_instance is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db_instance
