#!/usr/bin/env python3
"""Migrate historical JSON data into SQLite database."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure src is on path when run from repo root
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data.storage.db import Database, init_db
from data.storage.runtime_store import RuntimeStore


def main() -> int:
    print("=== JSON to SQLite Migration ===")

    db = init_db()
    store = RuntimeStore()

    counts: dict[str, int] = {}

    counts["manual_trades"] = db.migrate_manual_trades_from_json(store)
    counts["signals"] = db.migrate_signals_from_json(store)
    counts["signal_states"] = db.migrate_signal_states_from_json(store)
    counts["position_snapshots"] = db.migrate_position_snapshots_from_json(store)
    counts["backtests"] = db.migrate_backtests_from_json(store)
    counts["optimization_jobs"] = db.migrate_optimizations_from_json(store)
    counts["market_data_rows"] = db.migrate_market_data_from_parquet()

    print("\nMigration complete:")
    for name, count in counts.items():
        print(f"  {name}: {count}")

    # Sanity check: list tables and row counts
    print("\nDatabase summary:")
    import sqlite3

    conn = sqlite3.connect(db.db_path)
    conn.row_factory = sqlite3.Row
    tables = [
        "manual_trades",
        "signals",
        "signal_states",
        "position_snapshots",
        "backtests",
        "optimization_jobs",
        "market_data",
    ]
    for table in tables:
        row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table}").fetchone()
        print(f"  {table}: {row['cnt']} rows")
    conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
