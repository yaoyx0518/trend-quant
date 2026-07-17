"""API tests for the async rule backtest endpoints.

POST /rule-backtest/api/run starts a background thread and returns a run_id;
GET /rule-backtest/api/progress/{run_id} reports K-line-level progress and
carries the result once finished. The real engine is replaced by a fake
service so tests stay fast.
"""

from __future__ import annotations

import time

import pytest


def _install_fake_service(monkeypatch: pytest.MonkeyPatch, behavior: str) -> None:
    import app.routers.rule_backtest as rb_module

    class FakeRuleBacktestService:
        def run(self, payload: dict, progress_callback=None) -> dict:
            if behavior == "error":
                raise ValueError("symbol has no market data in range: TEST")
            if progress_callback is not None:
                progress_callback(1, 2)
                progress_callback(2, 2)
            return {
                "status": "ok",
                "run_id": "fake-run",
                "symbol": payload.get("symbol"),
                "results": [{"strategy_id": "s1", "trades": []}],
            }

    monkeypatch.setattr(rb_module, "RuleBacktestService", FakeRuleBacktestService)


def _poll_until_terminal(client, run_id: str, timeout_s: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        resp = client.get(f"/rule-backtest/api/progress/{run_id}")
        assert resp.status_code == 200
        data = resp.json()
        if data["status"] != "running":
            return data
        time.sleep(0.05)
    raise AssertionError("backtest job did not reach a terminal state in time")


class TestRuleBacktestAsyncApi:
    def test_run_then_poll_returns_progress_and_result(self, client, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_fake_service(monkeypatch, behavior="ok")

        start = client.post(
            "/rule-backtest/api/run",
            json={"strategy_ids": ["s1"], "symbol": "TEST", "start_date": "2026-01-01", "end_date": "2026-02-01"},
        )
        assert start.status_code == 200
        run_id = start.json()["run_id"]
        assert start.json()["status"] == "running"

        final = _poll_until_terminal(client, run_id)
        assert final["status"] == "ok"
        assert final["progress_current"] == final["progress_total"] == 2
        assert final["error"] is None
        assert final["result"]["symbol"] == "TEST"
        assert final["result"]["results"][0]["strategy_id"] == "s1"

    def test_run_with_service_value_error_surfaces_as_error_status(
        self, client, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_service(monkeypatch, behavior="error")

        start = client.post(
            "/rule-backtest/api/run",
            json={"strategy_ids": ["s1"], "symbol": "TEST"},
        )
        assert start.status_code == 200
        run_id = start.json()["run_id"]

        final = _poll_until_terminal(client, run_id)
        assert final["status"] == "error"
        assert "no market data in range" in final["error"]
        assert "result" not in final

    def test_progress_unknown_run_id_returns_404(self, client) -> None:
        resp = client.get("/rule-backtest/api/progress/does-not-exist")
        assert resp.status_code == 404
