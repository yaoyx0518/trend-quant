from __future__ import annotations

import threading
import time
from datetime import datetime

from backtest.optimization_engine import OptimizationEngine
from data.storage.runtime_store import RuntimeStore


class OptimizationJobManager:
    def __init__(self) -> None:
        self.runtime_store = RuntimeStore()
        self.engine = OptimizationEngine()
        self._jobs: dict[str, dict] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _new_job_id() -> str:
        return datetime.now().strftime("%Y%m%d%H%M%S%f")

    @staticmethod
    def _status_path(job_id: str) -> str:
        return f"optimizations/{job_id}/status.json"

    @staticmethod
    def _result_path(job_id: str) -> str:
        return f"optimizations/{job_id}/result.json"

    def discover_tunable_params(self) -> dict:
        return self.engine.discover_tunable_params()

    def _read_cancel_flag(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            return bool(job.get("cancel_requested", False))

    def _set_status(self, job_id: str, payload: dict, write_file: bool = True) -> dict:
        merged = {
            "job_id": job_id,
            "updated_at": datetime.now().isoformat(),
            **payload,
        }
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job["status_payload"] = merged
        if write_file:
            self.runtime_store.write_json(self._status_path(job_id), merged)
        return merged

    def _set_result(self, job_id: str, payload: dict) -> dict:
        merged = {
            "job_id": job_id,
            "updated_at": datetime.now().isoformat(),
            **payload,
        }
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job["result_payload"] = merged
        self.runtime_store.write_json(self._result_path(job_id), merged)
        return merged

    def start_job(self, payload: dict) -> dict:
        job_id = self._new_job_id()
        initial_status = {
            "status": "running",
            "created_at": datetime.now().isoformat(),
            "progress": {
                "done_units": 0,
                "total_units": 1,
                "percent": 0.0,
                "completed_eval_units": 0,
                "total_eval_units": 0,
                "workers": 1,
            },
            "summary": {},
            "current": {},
        }

        with self._lock:
            self._jobs[job_id] = {
                "cancel_requested": False,
                "status_payload": {
                    "job_id": job_id,
                    **initial_status,
                },
                "result_payload": None,
                "thread": None,
            }

        self.runtime_store.write_json(self._status_path(job_id), {"job_id": job_id, **initial_status})

        thread = threading.Thread(target=self._run_job, args=(job_id, payload), daemon=True)
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]["thread"] = thread
        thread.start()

        return {"job_id": job_id, "status": "running"}

    def _run_job(self, job_id: str, payload: dict) -> None:
        last_emit_ts = 0.0

        def is_cancelled() -> bool:
            return self._read_cancel_flag(job_id)

        def on_progress(update: dict) -> None:
            nonlocal last_emit_ts
            now_ts = time.time()

            with self._lock:
                current = dict((self._jobs.get(job_id) or {}).get("status_payload") or {})

            current.update(update or {})
            if "status" not in current:
                current["status"] = "running"

            # Keep UI updates responsive while limiting disk writes.
            write_file = (now_ts - last_emit_ts) >= 0.4
            self._set_status(job_id, current, write_file=write_file)
            if write_file:
                last_emit_ts = now_ts

        try:
            result = self.engine.run_optimization(payload=payload, is_cancelled=is_cancelled, on_progress=on_progress)
            finished_at = datetime.now().isoformat()
            final_status = {
                "status": str(result.get("status", "completed")),
                "created_at": (self.get_status(job_id) or {}).get("created_at", finished_at),
                "finished_at": finished_at,
                "progress": result.get("progress", {}),
                "summary": result.get("summary", {}),
                "current": {},
            }
            self._set_status(job_id, final_status, write_file=True)
            self._set_result(
                job_id,
                {
                    "status": result.get("status", "completed"),
                    "created_at": (self.get_status(job_id) or {}).get("created_at", finished_at),
                    "finished_at": finished_at,
                    "input": payload,
                    "summary": result.get("summary", {}),
                    "progress": result.get("progress", {}),
                    "best": result.get("best"),
                    "rows": result.get("rows", []),
                },
            )
        except Exception as exc:
            failed_at = datetime.now().isoformat()
            status_payload = {
                "status": "failed",
                "created_at": (self.get_status(job_id) or {}).get("created_at", failed_at),
                "finished_at": failed_at,
                "progress": {
                    "done_units": 0,
                    "total_units": 1,
                    "percent": 0.0,
                    "completed_eval_units": 0,
                    "total_eval_units": 0,
                    "workers": 1,
                },
                "summary": {},
                "error": str(exc),
            }
            self._set_status(job_id, status_payload, write_file=True)
            self._set_result(
                job_id,
                {
                    "status": "failed",
                    "created_at": status_payload.get("created_at"),
                    "finished_at": failed_at,
                    "input": payload,
                    "error": str(exc),
                    "rows": [],
                },
            )

    def cancel_job(self, job_id: str) -> dict:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                existing = self.runtime_store.read_json(self._status_path(job_id), default=None)
                if existing is None:
                    return {"job_id": job_id, "status": "not_found"}
                status_text = str(existing.get("status", ""))
                return {"job_id": job_id, "status": status_text or "unknown"}

            job["cancel_requested"] = True
            current = dict(job.get("status_payload") or {})

        if str(current.get("status", "")) in {"completed", "cancelled", "failed"}:
            return {"job_id": job_id, "status": current.get("status")}

        current["status"] = "cancelling"
        self._set_status(job_id, current, write_file=True)
        return {"job_id": job_id, "status": "cancelling"}

    def get_status(self, job_id: str) -> dict | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                return dict(job.get("status_payload") or {})
        return self.runtime_store.read_json(self._status_path(job_id), default=None)

    def get_result(self, job_id: str) -> dict | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None and job.get("result_payload") is not None:
                return dict(job.get("result_payload") or {})
        return self.runtime_store.read_json(self._result_path(job_id), default=None)
