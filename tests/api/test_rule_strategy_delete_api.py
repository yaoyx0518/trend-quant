"""API tests for the rule strategy delete endpoint.

DELETE /rule-backtest/api/strategies/{strategy_id} soft-deletes a strategy
in the DB (or removes the YAML file in YAML-only mode). These tests run
against the isolated test DB, which is seeded from config/rule_strategies
on first read.
"""

from __future__ import annotations


def _valid_strategy(strategy_id: str) -> dict:
    return {
        "schema_version": 1,
        "id": strategy_id,
        "name": f"测试策略 {strategy_id}",
        "trade_mode": "single_symbol_all_in",
        "entry": {
            "type": "group",
            "combinator": "all",
            "children": [
                {
                    "id": "c1",
                    "type": "condition",
                    "left": {"type": "price", "field": "close"},
                    "operator": ">=",
                    "right": {"type": "indicator", "name": "sma", "params": {"field": "close", "period": 20}},
                }
            ],
        },
        "exit": {
            "type": "group",
            "combinator": "all",
            "children": [
                {
                    "id": "c2",
                    "type": "condition",
                    "left": {"type": "price", "field": "close"},
                    "operator": "<=",
                    "right": {"type": "indicator", "name": "sma", "params": {"field": "close", "period": 20}},
                }
            ],
        },
    }


def _list_ids(client) -> list[str]:
    resp = client.get("/rule-backtest/api/meta")
    assert resp.status_code == 200
    return [item["id"] for item in resp.json()["strategies"]]


class TestRuleStrategyDeleteApi:
    def test_delete_seeded_strategy_removes_it_from_list(self, client) -> None:
        save = client.post(
            "/rule-backtest/api/strategies",
            json={"strategy": _valid_strategy("close_above_sma20"), "overwrite": False},
        )
        assert save.status_code == 200
        assert "close_above_sma20" in _list_ids(client)

        resp = client.delete("/rule-backtest/api/strategies/close_above_sma20")
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True
        assert resp.json()["id"] == "close_above_sma20"

        assert "close_above_sma20" not in _list_ids(client)

    def test_delete_unknown_strategy_returns_404(self, client) -> None:
        resp = client.delete("/rule-backtest/api/strategies/no-such-strategy")
        assert resp.status_code == 404

    def test_delete_invalid_id_returns_400(self, client) -> None:
        resp = client.delete("/rule-backtest/api/strategies/bad%20id")
        assert resp.status_code == 400

    def test_save_then_delete_roundtrip(self, client) -> None:
        save = client.post(
            "/rule-backtest/api/strategies",
            json={"strategy": _valid_strategy("del_target_1"), "overwrite": False},
        )
        assert save.status_code == 200
        assert "del_target_1" in _list_ids(client)

        resp = client.delete("/rule-backtest/api/strategies/del_target_1")
        assert resp.status_code == 200
        assert "del_target_1" not in _list_ids(client)

        # Deleting the same strategy again is a 404, not an error.
        resp = client.delete("/rule-backtest/api/strategies/del_target_1")
        assert resp.status_code == 404

    def test_delete_all_strategies_does_not_reseed_from_yaml(self, client) -> None:
        for sid in _list_ids(client):
            resp = client.delete(f"/rule-backtest/api/strategies/{sid}")
            assert resp.status_code == 200

        # Soft-deleted rows still exist, so the YAML seeding must not run
        # again and resurrect config/rule_strategies/*.yaml.
        assert _list_ids(client) == []
