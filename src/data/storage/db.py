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
        self._migrate()

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

                CREATE TABLE IF NOT EXISTS signal_run_locks (
                    run_key TEXT PRIMARY KEY,
                    trigger TEXT,
                    slot TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
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

                CREATE TABLE IF NOT EXISTS rule_strategies (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    schema_version INTEGER NOT NULL DEFAULT 1,
                    trade_mode TEXT NOT NULL DEFAULT 'single_symbol_all_in',
                    payload_json TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_rule_strategies_active_updated
                    ON rule_strategies(is_active, updated_at);

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

                CREATE TABLE IF NOT EXISTS market_data_raw (
                    symbol TEXT NOT NULL,
                    time TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    amount REAL,
                    provider TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, time)
                );
                CREATE INDEX IF NOT EXISTS idx_market_data_raw_symbol_time
                    ON market_data_raw(symbol, time);

                CREATE TABLE IF NOT EXISTS market_data_qfq (
                    symbol TEXT NOT NULL,
                    time TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    amount REAL,
                    provider TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, time)
                );
                CREATE INDEX IF NOT EXISTS idx_market_data_qfq_symbol_time
                    ON market_data_qfq(symbol, time);

                CREATE TABLE IF NOT EXISTS instrument_metadata (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    category_l1 TEXT,
                    category_l2 TEXT,
                    category_l3 TEXT,
                    factor_tags TEXT,
                    region_tag TEXT,
                    priority_l1 INTEGER,
                    priority_l2 INTEGER,
                    priority_l3 INTEGER,
                    sort_order INTEGER,
                    source TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_instrument_metadata_category
                    ON instrument_metadata(category_l1, category_l2, category_l3);
                CREATE INDEX IF NOT EXISTS idx_instrument_metadata_sort
                    ON instrument_metadata(priority_l1, priority_l2, priority_l3, sort_order, symbol);

                CREATE TABLE IF NOT EXISTS instrument_categories (
                    path TEXT PRIMARY KEY,
                    level INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    parent_path TEXT,
                    priority INTEGER,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_instrument_categories_parent
                    ON instrument_categories(parent_path, priority, name);

                """
            )

    def _migrate(self) -> None:
        with self._connect() as conn:
            try:
                conn.execute("ALTER TABLE backtests ADD COLUMN is_favorite INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            qfq_rows = conn.execute("SELECT COUNT(*) AS n FROM market_data_qfq").fetchone()
            legacy_rows = conn.execute("SELECT COUNT(*) AS n FROM market_data").fetchone()
            if (
                qfq_rows is not None
                and legacy_rows is not None
                and int(qfq_rows["n"] or 0) == 0
                and int(legacy_rows["n"] or 0) > 0
            ):
                conn.execute(
                    """INSERT OR IGNORE INTO market_data_qfq
                       (symbol, time, open, high, low, close, volume, amount, provider)
                       SELECT symbol, time, open, high, low, close, volume, amount, provider
                       FROM market_data"""
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
    def try_acquire_signal_run(self, run_key: str, trigger: str, slot: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT OR IGNORE INTO signal_run_locks (run_key, trigger, slot)
                   VALUES (?, ?, ?)""",
                (run_key, trigger, slot),
            )
            return cur.rowcount > 0

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
    # rule_strategies
    # ------------------------------------------------------------------
    def save_rule_strategy(self, strategy: dict, overwrite: bool = False) -> dict:
        strategy_id = str(strategy.get("id", "")).strip()
        if not strategy_id:
            raise ValueError("rule strategy id is required")

        with self._connect() as conn:
            if not overwrite:
                row = conn.execute(
                    "SELECT id FROM rule_strategies WHERE id = ? AND is_active = 1",
                    (strategy_id,),
                ).fetchone()
                if row:
                    raise FileExistsError(f"rule strategy already exists: {strategy_id}")

            conn.execute(
                """INSERT INTO rule_strategies
                   (id, name, description, schema_version, trade_mode, payload_json, is_active)
                   VALUES (?, ?, ?, ?, ?, ?, 1)
                   ON CONFLICT(id) DO UPDATE SET
                     name=excluded.name,
                     description=excluded.description,
                     schema_version=excluded.schema_version,
                     trade_mode=excluded.trade_mode,
                     payload_json=excluded.payload_json,
                     is_active=1,
                     updated_at=CURRENT_TIMESTAMP""",
                (
                    strategy_id,
                    str(strategy.get("name", strategy_id) or strategy_id),
                    str(strategy.get("description", "") or ""),
                    int(strategy.get("schema_version", 1) or 1),
                    str(strategy.get("trade_mode", "single_symbol_all_in") or "single_symbol_all_in"),
                    json.dumps(strategy, ensure_ascii=False),
                ),
            )
        saved = self.get_rule_strategy(strategy_id)
        if saved is None:
            raise RuntimeError(f"failed to save rule strategy: {strategy_id}")
        return saved

    def get_rule_strategy(self, strategy_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT * FROM rule_strategies
                   WHERE id = ? AND is_active = 1""",
                (strategy_id,),
            ).fetchone()
        return self._rule_strategy_row(row) if row else None

    def list_rule_strategies(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM rule_strategies
                   WHERE is_active = 1
                   ORDER BY updated_at DESC, id ASC"""
            ).fetchall()
        return [self._rule_strategy_row(row) for row in rows]

    def delete_rule_strategy(self, strategy_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE rule_strategies
                   SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                   WHERE id = ? AND is_active = 1""",
                (strategy_id,),
            )
            return cur.rowcount > 0

    @staticmethod
    def _rule_strategy_row(row: sqlite3.Row) -> dict:
        d = dict(row)
        payload = json.loads(d["payload_json"]) if d.get("payload_json") else {}
        d["strategy"] = payload
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

    def set_backtest_favorite(self, run_id: str, is_favorite: bool) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE backtests SET is_favorite = ? WHERE run_id = ?",
                (int(is_favorite), run_id),
            )
            return cur.rowcount > 0

    def delete_backtest(self, run_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM backtests WHERE run_id = ?", (run_id,))
            return cur.rowcount > 0

    def list_backtests(self, limit: int = 40) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT run_id, status, strategy_id, start_date, end_date,
                          total_return, win_rate, profit_factor, sharpe,
                          trade_count, timeline_days, summary, meta, input, is_favorite
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
                    "is_favorite": bool(d.get("is_favorite", 0)),
                }
            )
        return items

    def list_backtests_summary(self, limit: int = 40) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT run_id, status, start_date, end_date, total_return, is_favorite
                   FROM backtests
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
            return [
                {**dict(row), "is_favorite": bool(row["is_favorite"])}
                for row in rows
            ]

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
    # instrument_metadata
    # ------------------------------------------------------------------
    @staticmethod
    def _json_tags(value: Any) -> str:
        if isinstance(value, str):
            tags = [part.strip() for part in value.split("/") if part.strip()]
        elif isinstance(value, (list, tuple, set)):
            tags = [str(part).strip() for part in value if str(part).strip()]
        else:
            tags = []
        return json.dumps(tags, ensure_ascii=False)

    @staticmethod
    def _parse_tags(value: Any) -> list[str]:
        if value is None or value == "":
            return []
        try:
            parsed = json.loads(str(value))
        except json.JSONDecodeError:
            return [part.strip() for part in str(value).split("/") if part.strip()]
        if isinstance(parsed, list):
            return [str(part).strip() for part in parsed if str(part).strip()]
        return []

    @staticmethod
    def _category_path(row: dict[str, Any]) -> str:
        parts = [
            str(row.get("category_l1") or "").strip(),
            str(row.get("category_l2") or "").strip(),
            str(row.get("category_l3") or "").strip(),
        ]
        return "-".join(part for part in parts if part)

    @staticmethod
    def _metadata_row_to_dict(row: sqlite3.Row) -> dict:
        item = dict(row)
        item["factor_tags"] = Database._parse_tags(item.get("factor_tags"))
        item["category_path"] = Database._category_path(item)
        return item

    def save_instrument_metadata(self, items: list[dict[str, Any]]) -> int:
        records: list[tuple] = []
        for item in items:
            symbol = str(item.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            records.append(
                (
                    symbol,
                    str(item.get("name") or "").strip(),
                    str(item.get("category_l1") or "").strip(),
                    str(item.get("category_l2") or "").strip(),
                    str(item.get("category_l3") or "").strip(),
                    self._json_tags(item.get("factor_tags")),
                    str(item.get("region_tag") or "").strip(),
                    item.get("priority_l1"),
                    item.get("priority_l2"),
                    item.get("priority_l3"),
                    item.get("sort_order"),
                    str(item.get("source") or "").strip(),
                )
            )
        if not records:
            return 0

        with self._connect() as conn:
            conn.executemany(
                """INSERT INTO instrument_metadata
                   (symbol, name, category_l1, category_l2, category_l3, factor_tags,
                    region_tag, priority_l1, priority_l2, priority_l3, sort_order, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(symbol) DO UPDATE SET
                     name=excluded.name,
                     category_l1=excluded.category_l1,
                     category_l2=excluded.category_l2,
                     category_l3=excluded.category_l3,
                     factor_tags=excluded.factor_tags,
                     region_tag=excluded.region_tag,
                     priority_l1=excluded.priority_l1,
                     priority_l2=excluded.priority_l2,
                     priority_l3=excluded.priority_l3,
                     sort_order=excluded.sort_order,
                     source=excluded.source,
                     updated_at=CURRENT_TIMESTAMP""",
                records,
            )
        return len(records)

    def list_instrument_metadata(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM instrument_metadata
                   ORDER BY
                     priority_l1 IS NULL, priority_l1,
                     priority_l2 IS NULL, priority_l2,
                     priority_l3 IS NULL, priority_l3,
                     sort_order IS NULL, sort_order,
                     symbol"""
            ).fetchall()
        return [self._metadata_row_to_dict(row) for row in rows]

    def get_instrument_metadata(self, symbol: str) -> dict | None:
        normalized = str(symbol or "").strip().upper()
        if not normalized:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM instrument_metadata WHERE symbol = ?",
                (normalized,),
            ).fetchone()
        return self._metadata_row_to_dict(row) if row else None

    def get_instrument_metadata_map(self) -> dict[str, dict]:
        return {item["symbol"]: item for item in self.list_instrument_metadata()}

    def load_market_dashboard_history(self, days: int = 90) -> list[dict]:
        """Return recent adjusted daily bars for fully classified managed instruments."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT symbol, name, time, open, high, low, close, volume, amount,
                          category_l1, category_l2, category_l3,
                          priority_l1, priority_l2, priority_l3, sort_order
                   FROM (
                       SELECT d.symbol, m.name, d.time, d.open, d.high, d.low, d.close, d.volume, d.amount,
                              m.category_l1, m.category_l2, m.category_l3,
                              m.priority_l1, m.priority_l2, m.priority_l3, m.sort_order,
                              ROW_NUMBER() OVER (PARTITION BY d.symbol ORDER BY d.time DESC) AS rn
                       FROM market_data_qfq d
                       JOIN instrument_metadata m ON m.symbol = d.symbol
                       WHERE TRIM(COALESCE(m.category_l1, '')) <> ''
                         AND TRIM(COALESCE(m.category_l2, '')) <> ''
                         AND TRIM(COALESCE(m.category_l3, '')) <> ''
                   )
                   WHERE rn <= ?
                   ORDER BY category_l1, category_l2, category_l3, symbol, time""",
                (max(1, int(days)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_market_dashboard_revision(self) -> tuple[str, int, str]:
        """Small revision token used to invalidate the in-process subject-board cache."""
        with self._connect() as conn:
            market = conn.execute(
                "SELECT MAX(time) AS latest_time, COUNT(*) AS row_count FROM market_data_qfq"
            ).fetchone()
            metadata = conn.execute(
                "SELECT MAX(updated_at) AS latest_metadata FROM instrument_metadata"
            ).fetchone()
        return (
            str(market["latest_time"] or "") if market else "",
            int(market["row_count"] or 0) if market else 0,
            str(metadata["latest_metadata"] or "") if metadata else "",
        )

    def save_instrument_categories(self, categories: list[dict[str, Any]]) -> int:
        records: list[tuple] = []
        for item in categories:
            path = str(item.get("path") or "").strip()
            name = str(item.get("name") or "").strip()
            if not path or not name:
                continue
            records.append(
                (
                    path,
                    int(item.get("level") or 0),
                    name,
                    str(item.get("parent_path") or "").strip() or None,
                    item.get("priority"),
                )
            )
        if not records:
            return 0

        with self._connect() as conn:
            conn.executemany(
                """INSERT INTO instrument_categories
                   (path, level, name, parent_path, priority)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(path) DO UPDATE SET
                     level=excluded.level,
                     name=excluded.name,
                     parent_path=excluded.parent_path,
                     priority=excluded.priority,
                     updated_at=CURRENT_TIMESTAMP""",
                records,
            )
        return len(records)

    def replace_instrument_categories(self, categories: list[dict[str, Any]]) -> int:
        with self._connect() as conn:
            conn.execute("DELETE FROM instrument_categories")
        return self.save_instrument_categories(categories)

    def list_instrument_categories(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM instrument_categories
                   ORDER BY level, parent_path IS NULL DESC, parent_path, priority IS NULL, priority, name"""
            ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # market_data
    # ------------------------------------------------------------------
    @staticmethod
    def _market_table(price_mode: str = "qfq") -> str:
        value = str(price_mode or "qfq").strip().lower()
        if value in {"qfq", "forward", "forward_additive"}:
            return "market_data_qfq"
        if value in {"raw", "none", "unadjusted"}:
            return "market_data_raw"
        if value in {"legacy", "market_data"}:
            return "market_data"
        raise ValueError(f"unsupported market data price_mode: {price_mode}")

    def save_market_data(self, symbol: str, df, price_mode: str = "qfq") -> None:
        if df.empty:
            return
        table = self._market_table(price_mode)
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
                f"""INSERT OR REPLACE INTO {table}
                   (symbol, time, open, high, low, close, volume, amount, provider)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                records,
            )

    def load_market_data(self, symbol: str, price_mode: str = "qfq"):
        import pandas as pd

        table = self._market_table(price_mode)
        with self._connect() as conn:
            rows = conn.execute(
                f"""SELECT time, open, high, low, close, volume, amount, symbol, provider
                   FROM {table} WHERE symbol = ? ORDER BY time""",
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

    def list_market_symbols(self, price_mode: str = "qfq") -> list[str]:
        table = self._market_table(price_mode)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT DISTINCT symbol FROM {table} ORDER BY symbol"
            ).fetchall()
            return [r["symbol"] for r in rows]

    def get_market_data_summary(self, symbol: str, price_mode: str = "qfq") -> dict:
        table = self._market_table(price_mode)
        with self._connect() as conn:
            row = conn.execute(
                f"""SELECT COUNT(*) AS rows, MIN(time) AS start, MAX(time) AS end
                   FROM {table} WHERE symbol = ?""",
                (symbol,),
            ).fetchone()
        if row is None or row["rows"] == 0:
            return {"rows": 0, "start": None, "end": None}
        return {"rows": row["rows"], "start": row["start"], "end": row["end"]}

    def clear_market_data(self, price_mode: str = "qfq") -> int:
        table = self._market_table(price_mode)
        with self._connect() as conn:
            cur = conn.execute(f"DELETE FROM {table}")
            return int(cur.rowcount or 0)

    def migrate_market_data_from_parquet(self, base_dir: str = "data/market/etf") -> int:
        import pandas as pd
        from pathlib import Path

        count = 0
        for p in Path(base_dir).glob("*.parquet"):
            df = pd.read_parquet(p)
            if not df.empty:
                self.save_market_data(p.stem, df, price_mode="qfq")
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
