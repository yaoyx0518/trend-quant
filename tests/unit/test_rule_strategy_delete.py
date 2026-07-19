"""Unit tests for StrategyLoader.delete in YAML-file mode (no DB)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rule_backtest.loader import StrategyLoader


def _write_strategy(dir_path: Path, strategy_id: str) -> Path:
    payload = {
        "schema_version": 1,
        "id": strategy_id,
        "name": f"strategy {strategy_id}",
        "trade_mode": "single_symbol_all_in",
        "entry": {"type": "group", "combinator": "all", "children": []},
        "exit": {"type": "group", "combinator": "all", "children": []},
    }
    path = dir_path / f"{strategy_id}.yaml"
    path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")
    return path


class TestStrategyLoaderDeleteYaml:
    def test_delete_removes_matching_yaml_file(self, tmp_path: Path) -> None:
        _write_strategy(tmp_path, "keep_me")
        target = _write_strategy(tmp_path, "delete_me")
        loader = StrategyLoader(base_dir=tmp_path, use_db=False)

        result = loader.delete("delete_me")

        assert result["deleted"] is True
        assert result["storage"] == "yaml"
        assert not target.exists()
        assert (tmp_path / "keep_me.yaml").exists()
        assert [s["id"] for s in loader.list_strategies()] == ["keep_me"]

    def test_delete_unknown_id_raises_file_not_found(self, tmp_path: Path) -> None:
        _write_strategy(tmp_path, "keep_me")
        loader = StrategyLoader(base_dir=tmp_path, use_db=False)

        with pytest.raises(FileNotFoundError):
            loader.delete("no_such_strategy")

    @pytest.mark.parametrize("bad_id", ["", "bad id", "../escape", "a/b"])
    def test_delete_rejects_invalid_id(self, tmp_path: Path, bad_id: str) -> None:
        loader = StrategyLoader(base_dir=tmp_path, use_db=False)

        with pytest.raises(ValueError):
            loader.delete(bad_id)
