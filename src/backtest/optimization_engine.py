from __future__ import annotations

import ctypes
import math
import os
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from datetime import datetime
from decimal import Decimal, InvalidOperation
from itertools import product
from pathlib import Path
from statistics import median, pstdev
from typing import Callable

import yaml

from backtest.backtest_engine import BacktestEngine


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _split_combo(combo: dict[str, float | int]) -> tuple[dict[str, float | int], dict[str, float | int]]:
    strategy_overrides: dict[str, float | int] = {}
    instrument_overrides: dict[str, float | int] = {}
    for key, value in combo.items():
        if key.startswith("strategy."):
            strategy_overrides[key.split(".", 1)[1]] = value
        elif key.startswith("instruments."):
            instrument_overrides[key.split(".", 1)[1]] = value
    return strategy_overrides, instrument_overrides


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _evaluate_window_task(task: dict) -> dict:
    combo = task.get("combo", {}) or {}
    window = task.get("window", {}) or {}
    initial_capital = float(task.get("initial_capital", 200000.0) or 200000.0)

    strategy_overrides, instrument_overrides = _split_combo(combo)
    payload = {
        "start_date": str(window.get("start_date", "")),
        "end_date": str(window.get("end_date", "")),
        "initial_capital": initial_capital,
    }

    try:
        engine = BacktestEngine()
        result = engine.run(
            payload,
            strategy_overrides=strategy_overrides,
            instrument_overrides=instrument_overrides,
            persist=False,
            include_charts=False,
            include_trades=False,
        )
        summary = result.get("summary", {}) if isinstance(result, dict) else {}
        return {
            "combo_id": int(task.get("combo_id", -1)),
            "window_index": int(task.get("window_index", -1)),
            "window": payload,
            "status": str(result.get("status", "failed")),
            "sharpe": _safe_float(summary.get("sharpe", 0.0), 0.0),
            "max_drawdown": _safe_float(summary.get("max_drawdown", 0.0), 0.0),
            "trade_count": int(_safe_float(summary.get("trade_count", 0.0), 0.0)),
            "win_rate": _safe_float(summary.get("win_rate", 0.0), 0.0),
            "profit_factor": _safe_float(summary.get("profit_factor", 0.0), 0.0),
            "annual_return": _safe_float(summary.get("annual_return", 0.0), 0.0),
            "total_return": _safe_float(summary.get("total_return", 0.0), 0.0),
        }
    except Exception as exc:
        return {
            "combo_id": int(task.get("combo_id", -1)),
            "window_index": int(task.get("window_index", -1)),
            "window": payload,
            "status": "error",
            "error": str(exc),
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "trade_count": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "annual_return": 0.0,
            "total_return": 0.0,
        }


class OptimizationEngine:
    TOP_N_ROWS = 50

    def __init__(self) -> None:
        self._strategy_path = Path("config/strategy.yaml")
        self._instruments_path = Path("config/instruments.yaml")

    @staticmethod
    def _load_yaml(path: Path, key: str) -> dict | list:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data.get(key, {} if key == "strategy" else [])

    def discover_tunable_params(self) -> dict:
        strategy_cfg = self._load_yaml(self._strategy_path, "strategy")
        instruments_cfg = self._load_yaml(self._instruments_path, "instruments")
        enabled = [item for item in instruments_cfg if bool(item.get("enabled", True))]

        params: list[dict] = []

        for key, value in strategy_cfg.items():
            if not _is_number(value):
                continue
            is_int = isinstance(value, int) and not isinstance(value, bool)
            params.append(
                {
                    "key": f"strategy.{key}",
                    "source": "strategy",
                    "name": key,
                    "value_type": "int" if is_int else "float",
                    "current": value,
                    "step_suggest": 1 if is_int else 0.1,
                }
            )

        if enabled:
            common_keys: set[str] | None = None
            for item in enabled:
                numeric_keys = {k for k, v in item.items() if _is_number(v)}
                common_keys = numeric_keys if common_keys is None else (common_keys & numeric_keys)

            for key in sorted(common_keys or set()):
                values = [item.get(key) for item in enabled]
                if not values:
                    continue
                if any(not _is_number(v) for v in values):
                    continue
                is_int = all(isinstance(v, int) and not isinstance(v, bool) for v in values)
                unique_values = sorted({float(v) for v in values})
                params.append(
                    {
                        "key": f"instruments.{key}",
                        "source": "instruments",
                        "name": key,
                        "value_type": "int" if is_int else "float",
                        "current": values[0],
                        "is_uniform": len(unique_values) == 1,
                        "all_values": unique_values,
                        "step_suggest": 1 if is_int else 0.1,
                    }
                )

        params_sorted = sorted(params, key=lambda x: (x.get("source", ""), x.get("name", "")))
        return {
            "items": params_sorted,
            "count": len(params_sorted),
        }

    @staticmethod
    def _expand_values(range_cfg: dict, value_type: str) -> list[float | int]:
        try:
            v_min = Decimal(str(range_cfg.get("min")))
            v_max = Decimal(str(range_cfg.get("max")))
            v_step = Decimal(str(range_cfg.get("step")))
        except (InvalidOperation, TypeError):
            raise ValueError("invalid_range_number")

        if v_step <= 0:
            raise ValueError("step_must_be_positive")
        if v_max < v_min:
            raise ValueError("max_must_be_greater_or_equal_min")

        values: list[float | int] = []
        current = v_min
        guard = 0
        while current <= v_max:
            if value_type == "int":
                values.append(int(round(float(current))))
            else:
                values.append(float(current))
            current += v_step
            guard += 1
            if guard > 200000:
                raise ValueError("range_too_large")

        if not values:
            raise ValueError("empty_range")

        # Keep monotonic unique values after integer rounding.
        uniq: list[float | int] = []
        seen: set[float | int] = set()
        for item in values:
            if item in seen:
                continue
            seen.add(item)
            uniq.append(item)
        return uniq

    @staticmethod
    def _merge_strategy_for_validation(base_strategy: dict, combo: dict[str, float | int]) -> dict:
        merged = dict(base_strategy)
        for key, value in combo.items():
            if key.startswith("strategy."):
                merged[key.split(".", 1)[1]] = value
        return merged

    @staticmethod
    def _validate_combo_constraints(strategy_cfg: dict) -> list[str]:
        reasons: list[str] = []

        n_short = strategy_cfg.get("n_short")
        n_mid = strategy_cfg.get("n_mid")
        n_long = strategy_cfg.get("n_long")
        if _is_number(n_short) and _is_number(n_mid) and _is_number(n_long):
            if not (float(n_short) < float(n_mid) < float(n_long)):
                reasons.append("invalid_ma_order")

        entry = strategy_cfg.get("entry_threshold")
        entry_min = strategy_cfg.get("entry_threshold_min", entry)
        entry_max = strategy_cfg.get("entry_threshold_max")
        if _is_number(entry_min) and _is_number(entry_max):
            if float(entry_min) > float(entry_max):
                reasons.append("invalid_entry_threshold_range")

        return reasons

    @staticmethod
    def _compute_score(sharpes: list[float]) -> tuple[float | None, float | None, float | None, float | None]:
        if not sharpes:
            return None, None, None, None
        med = float(median(sharpes))
        min_s = float(min(sharpes))
        std_s = float(pstdev(sharpes)) if len(sharpes) > 1 else 0.0
        score = 0.6 * med + 0.3 * min_s - 0.1 * std_s
        return score, med, min_s, std_s

    @staticmethod
    def _compute_loo(window_results: list[dict], total_windows: int) -> dict:
        sharpe_by_idx = {
            int(item.get("window_index", -1)): float(item.get("sharpe", 0.0))
            for item in window_results
            if str(item.get("status", "")) == "ok"
        }

        folds: list[dict] = []
        for left_out in range(total_windows):
            validate = sharpe_by_idx.get(left_out)
            train = [v for idx, v in sharpe_by_idx.items() if idx != left_out]
            if validate is None or not train:
                folds.append(
                    {
                        "left_out_window_index": left_out,
                        "train_score": None,
                        "validate_sharpe": validate,
                    }
                )
                continue
            train_score, _, _, _ = OptimizationEngine._compute_score(train)
            folds.append(
                {
                    "left_out_window_index": left_out,
                    "train_score": train_score,
                    "validate_sharpe": float(validate),
                }
            )

        validate_values = [float(x["validate_sharpe"]) for x in folds if x.get("validate_sharpe") is not None]
        train_scores = [float(x["train_score"]) for x in folds if x.get("train_score") is not None]

        return {
            "folds": folds,
            "validate_sharpe_median": float(median(validate_values)) if validate_values else None,
            "train_score_median": float(median(train_scores)) if train_scores else None,
        }

    @staticmethod
    def _available_memory_gb() -> float:
        try:
            if os.name == "nt":
                class MEMORYSTATUSEX(ctypes.Structure):
                    _fields_ = [
                        ("dwLength", ctypes.c_ulong),
                        ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong),
                        ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong),
                        ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong),
                        ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
                    ]

                stat = MEMORYSTATUSEX()
                stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
                if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
                    return float(stat.ullAvailPhys) / (1024.0 ** 3)
                return 8.0

            page_size = os.sysconf("SC_PAGE_SIZE")
            avail_pages = os.sysconf("SC_AVPHYS_PAGES")
            return float(page_size * avail_pages) / (1024.0 ** 3)
        except Exception:
            return 8.0

    @classmethod
    def resolve_worker_count(cls, parallel_mode: str, manual_workers: int | None = None) -> int:
        mode = str(parallel_mode or "auto").lower()
        if mode == "single":
            return 1
        if mode == "manual":
            return max(1, min(int(manual_workers or 1), 32))

        cpu = int(os.cpu_count() or 4)
        avail_mem_gb = cls._available_memory_gb()
        cpu_half = max(2, math.floor(cpu * 0.5))
        mem_limit = max(2, math.floor(avail_mem_gb / 1.2))
        workers = min(10, max(2, cpu_half, mem_limit))
        return max(1, workers)

    def run_optimization(
        self,
        payload: dict,
        is_cancelled: Callable[[], bool],
        on_progress: Callable[[dict], None],
    ) -> dict:
        selected_params = payload.get("selected_params", []) or []
        windows = payload.get("windows", []) or []

        if not selected_params:
            raise ValueError("selected_params is required")
        if not windows:
            raise ValueError("windows is required")

        discovery = self.discover_tunable_params()
        meta_map = {item["key"]: item for item in discovery.get("items", [])}

        key_order: list[str] = []
        value_grid: list[list[float | int]] = []
        for item in selected_params:
            key = str(item.get("key", "")).strip()
            if key == "":
                raise ValueError("selected_params contains empty key")
            meta = meta_map.get(key)
            if meta is None:
                raise ValueError(f"unsupported_param:{key}")
            values = self._expand_values(item, str(meta.get("value_type", "float")))
            key_order.append(key)
            value_grid.append(values)

        raw_combo_count = 1
        for values in value_grid:
            raw_combo_count *= max(1, len(values))

        strategy_cfg = self._load_yaml(self._strategy_path, "strategy")

        valid_combos: list[dict] = []
        invalid_combo_count = 0
        invalid_combo_reasons: dict[str, int] = {}

        for row in product(*value_grid):
            combo = {key_order[i]: row[i] for i in range(len(key_order))}
            merged_strategy = self._merge_strategy_for_validation(strategy_cfg, combo)
            reasons = self._validate_combo_constraints(merged_strategy)
            if reasons:
                invalid_combo_count += 1
                for reason in reasons:
                    invalid_combo_reasons[reason] = invalid_combo_reasons.get(reason, 0) + 1
                continue
            valid_combos.append(combo)

        total_windows = len(windows)
        enable_loo = bool(payload.get("enable_loo", False))
        total_eval_units = len(valid_combos) * total_windows
        loo_units = len(valid_combos) * total_windows if enable_loo else 0
        total_units = total_eval_units + loo_units

        done_units = 0
        workers = self.resolve_worker_count(str(payload.get("parallel_mode", "auto")), payload.get("manual_workers"))

        combo_windows: dict[int, list[dict | None]] = {idx: [None for _ in range(total_windows)] for idx in range(len(valid_combos))}

        on_progress(
            {
                "status": "running",
                "progress": {
                    "done_units": 0,
                    "total_units": max(total_units, 1),
                    "percent": 0.0,
                    "completed_eval_units": 0,
                    "total_eval_units": total_eval_units,
                    "workers": workers,
                },
                "meta": {
                    "raw_combo_count": raw_combo_count,
                    "valid_combo_count": len(valid_combos),
                    "invalid_combo_count": invalid_combo_count,
                    "invalid_combo_reasons": invalid_combo_reasons,
                },
            }
        )

        tasks: list[dict] = []
        initial_capital = float(payload.get("initial_capital", 200000.0) or 200000.0)
        for combo_id, combo in enumerate(valid_combos):
            for window_idx, window in enumerate(windows):
                tasks.append(
                    {
                        "combo_id": combo_id,
                        "combo": combo,
                        "window_index": window_idx,
                        "window": {
                            "start_date": str(window.get("start_date", "")),
                            "end_date": str(window.get("end_date", "")),
                        },
                        "initial_capital": initial_capital,
                    }
                )

        cancelled = False
        submitted = 0
        max_in_flight = max(2, workers * 2)
        future_map: dict = {}

        executor_type = "process"
        if tasks:
            if workers <= 1:
                executor_type = "thread"
                executor_obj = ThreadPoolExecutor(max_workers=workers)
            else:
                try:
                    executor_obj = ProcessPoolExecutor(max_workers=workers)
                except Exception:
                    executor_type = "thread"
                    executor_obj = ThreadPoolExecutor(max_workers=workers)

            with executor_obj as executor:
                while submitted < len(tasks) and len(future_map) < max_in_flight:
                    future = executor.submit(_evaluate_window_task, tasks[submitted])
                    future_map[future] = tasks[submitted]
                    submitted += 1

                while future_map:
                    if is_cancelled():
                        cancelled = True
                        on_progress(
                            {
                                "status": "cancelling",
                                "progress": {
                                    "done_units": done_units,
                                    "total_units": max(total_units, 1),
                                    "percent": float(done_units) / max(total_units, 1),
                                    "completed_eval_units": done_units,
                                    "total_eval_units": total_eval_units,
                                    "workers": workers,
                                },
                            }
                        )
                        for future in list(future_map.keys()):
                            future.cancel()

                    done, _pending = wait(list(future_map.keys()), timeout=0.5, return_when=FIRST_COMPLETED)
                    if not done:
                        continue

                    for future in done:
                        task = future_map.pop(future)
                        if future.cancelled():
                            continue

                        try:
                            result = future.result()
                        except Exception as exc:
                            result = {
                                "combo_id": int(task.get("combo_id", -1)),
                                "window_index": int(task.get("window_index", -1)),
                                "window": task.get("window", {}),
                                "status": "error",
                                "error": str(exc),
                                "sharpe": 0.0,
                                "max_drawdown": 0.0,
                                "trade_count": 0,
                                "annual_return": 0.0,
                                "total_return": 0.0,
                            }

                        combo_id = int(result.get("combo_id", -1))
                        window_index = int(result.get("window_index", -1))
                        if combo_id >= 0 and window_index >= 0 and combo_id in combo_windows and window_index < total_windows:
                            combo_windows[combo_id][window_index] = result

                        done_units += 1
                        on_progress(
                            {
                                "status": "cancelling" if cancelled else "running",
                                "progress": {
                                    "done_units": done_units,
                                    "total_units": max(total_units, 1),
                                    "percent": float(done_units) / max(total_units, 1),
                                    "completed_eval_units": done_units,
                                    "total_eval_units": total_eval_units,
                                    "workers": workers,
                                },
                                "current": {
                                    "combo_id": combo_id,
                                    "window_index": window_index,
                                    "window": result.get("window", {}),
                                    "combo": valid_combos[combo_id] if 0 <= combo_id < len(valid_combos) else {},
                                },
                            }
                        )

                        while (not cancelled) and submitted < len(tasks) and len(future_map) < max_in_flight:
                            next_future = executor.submit(_evaluate_window_task, tasks[submitted])
                            future_map[next_future] = tasks[submitted]
                            submitted += 1

                    if cancelled and submitted >= len(tasks) and not future_map:
                        break

        rows: list[dict] = []
        hard_filter_reason_counts = {
            "dd_exceed": 0,
            "trade_too_few": 0,
            "min_sharpe_non_positive": 0,
        }

        # LOO calculation is modeled as additional units so progress remains consistent with configured workload.
        loo_done = 0

        for combo_id, combo in enumerate(valid_combos):
            window_results = [x for x in (combo_windows.get(combo_id) or []) if x is not None]
            ok_results = [x for x in window_results if str(x.get("status", "")) == "ok"]
            sharpe_values = [float(x.get("sharpe", 0.0)) for x in ok_results]
            max_dd_values = [float(x.get("max_drawdown", 0.0)) for x in ok_results]
            trade_count_values = [int(x.get("trade_count", 0)) for x in ok_results]
            win_rate_values = [float(x.get("win_rate", 0.0)) for x in ok_results]
            profit_factor_values = [float(x.get("profit_factor", 0.0)) for x in ok_results]

            score, median_sharpe, min_sharpe, std_sharpe = self._compute_score(sharpe_values)
            worst_dd = min(max_dd_values) if max_dd_values else None
            trade_count_total = int(sum(trade_count_values))
            median_win_rate = float(median(win_rate_values)) if win_rate_values else None
            median_profit_factor = float(median(profit_factor_values)) if profit_factor_values else None

            hard_reasons: list[str] = []
            if ok_results:
                if worst_dd is not None and worst_dd < -0.15:
                    hard_reasons.append("dd_exceed")
                if trade_count_total < 30:
                    hard_reasons.append("trade_too_few")
                if min_sharpe is None or min_sharpe <= 0:
                    hard_reasons.append("min_sharpe_non_positive")

            for reason in hard_reasons:
                if reason in hard_filter_reason_counts:
                    hard_filter_reason_counts[reason] += 1

            is_partial = len(window_results) < total_windows
            coverage_rate = (float(len(window_results)) / float(total_windows)) if total_windows > 0 else 0.0
            hard_filter_passed = bool(ok_results) and (len(hard_reasons) == 0)

            loo_info: dict = {}
            if enable_loo:
                loo_info = self._compute_loo(ok_results, total_windows)
                for _ in range(total_windows):
                    loo_done += 1
                    done_for_loo = done_units + loo_done
                    on_progress(
                        {
                            "status": "cancelling" if cancelled else "running",
                            "progress": {
                                "done_units": done_for_loo,
                                "total_units": max(total_units, 1),
                                "percent": float(done_for_loo) / max(total_units, 1),
                                "completed_eval_units": done_units,
                                "total_eval_units": total_eval_units,
                                "workers": workers,
                            },
                        }
                    )

            row = {
                "rank": 0,
                "score": score,
                "is_partial": is_partial,
                "coverage_rate": coverage_rate,
                "completed_windows": len(window_results),
                "total_windows": total_windows,
                "median_sharpe": median_sharpe,
                "min_sharpe": min_sharpe,
                "std_sharpe": std_sharpe,
                "max_drawdown_worst": worst_dd,
                "trade_count_total": trade_count_total,
                "win_rate": median_win_rate,
                "profit_factor": median_profit_factor,
                "hard_filter_passed": hard_filter_passed,
                "hard_filter_reasons": hard_reasons,
                "params": combo,
                "window_metrics": sorted(ok_results, key=lambda x: int(x.get("window_index", 0))),
            }
            if enable_loo:
                row["loo"] = loo_info
            rows.append(row)

        rows.sort(
            key=lambda x: (
                x.get("score") is None,
                -float(x.get("score", -999999.0) or -999999.0),
                x.get("is_partial", False),
            )
        )
        for idx, row in enumerate(rows, start=1):
            row["rank"] = idx

        recommended_idx = None
        for idx, row in enumerate(rows):
            if bool(row.get("hard_filter_passed")) and not bool(row.get("is_partial")):
                recommended_idx = idx
                break
        if recommended_idx is None:
            for idx, row in enumerate(rows):
                if bool(row.get("hard_filter_passed")):
                    recommended_idx = idx
                    break
        if recommended_idx is None and rows:
            recommended_idx = 0

        for idx, row in enumerate(rows):
            row["recommended"] = bool(recommended_idx is not None and idx == recommended_idx)

        hard_filtered_count = sum(1 for row in rows if (row.get("completed_windows", 0) > 0 and not row.get("hard_filter_passed", False)))
        partial_count = sum(1 for row in rows if row.get("is_partial", False))
        completed_count = sum(1 for row in rows if not row.get("is_partial", False))
        evaluated_count = sum(1 for row in rows if row.get("completed_windows", 0) > 0)
        total_ranked_rows = len(rows)
        rows = rows[: self.TOP_N_ROWS]

        status = "cancelled" if cancelled or is_cancelled() else "completed"
        final_done_units = done_units + loo_done

        return {
            "status": status,
            "created_at": datetime.now().isoformat(),
            "summary": {
                "raw_combo_count": raw_combo_count,
                "valid_combo_count": len(valid_combos),
                "invalid_combo_count": invalid_combo_count,
                "invalid_combo_reasons": invalid_combo_reasons,
                "evaluated_combo_count": evaluated_count,
                "completed_combo_count": completed_count,
                "partial_combo_count": partial_count,
                "hard_filtered_count": hard_filtered_count,
                "hard_filter_reason_counts": hard_filter_reason_counts,
                "enable_loo": enable_loo,
                "workers": workers,
                "ranked_rows_total": total_ranked_rows,
                "ranked_rows_kept": len(rows),
            },
            "progress": {
                "done_units": final_done_units,
                "total_units": max(total_units, 1),
                "percent": min(1.0, float(final_done_units) / max(total_units, 1)),
                "completed_eval_units": done_units,
                "total_eval_units": total_eval_units,
                "workers": workers,
            },
            "rows": rows,
            "best": rows[recommended_idx] if (recommended_idx is not None and 0 <= recommended_idx < len(rows)) else None,
        }

