"""Unit tests for strategy.catalog — strategy ID normalization and config resolution."""

from __future__ import annotations

import pytest

from strategy.catalog import (
    normalize_strategy_id,
    resolve_strategy_config,
    build_strategy_catalog,
    TREND_STRATEGY_ID,
    MOMENTUM_STRATEGY_ID,
    MOMENTUM_STRATEGY_V2_ID,
    MOMENTUM_STRATEGY_V3_ID,
    MOMENTUM_STRATEGY_IDS,
)


class TestNormalizeStrategyId:
    def test_known_ids_pass_through(self) -> None:
        assert normalize_strategy_id("trend_score_v1") == "trend_score_v1"
        assert normalize_strategy_id("momentum_topn_v1") == "momentum_topn_v1"
        assert normalize_strategy_id("momentum_topn_v2") == "momentum_topn_v2"
        assert normalize_strategy_id("momentum_topn_v3") == "momentum_topn_v3"

    def test_unknown_falls_back(self) -> None:
        assert normalize_strategy_id("unknown_strategy") == TREND_STRATEGY_ID
        assert normalize_strategy_id("") == TREND_STRATEGY_ID
        assert normalize_strategy_id(None) == TREND_STRATEGY_ID

    def test_custom_fallback(self) -> None:
        assert normalize_strategy_id("unknown", fallback="momentum_topn_v1") == "momentum_topn_v1"


class TestResolveStrategyConfig:
    def _trend_cfg(self) -> dict:
        """Return a minimal strategy config dict (mirrors strategy.yaml)."""
        return {
            "n_short": 5,
            "n_mid": 10,
            "n_long": 20,
            "atr_period": 20,
            "vol_ma_period": 20,
            "er_period": 10,
        }

    def test_trend_defaults(self) -> None:
        cfg = resolve_strategy_config(self._trend_cfg(), TREND_STRATEGY_ID)
        assert cfg["n_short"] == 5
        assert cfg["n_mid"] == 10
        assert cfg["n_long"] == 20
        assert cfg["atr_period"] == 20

    def test_momentum_v1_defaults(self) -> None:
        cfg = resolve_strategy_config({}, MOMENTUM_STRATEGY_ID)
        assert "max_holdings" in cfg
        assert "buy_filters" in cfg
        assert "sell_signals" in cfg

    def test_momentum_v2_extends_v1(self) -> None:
        cfg = resolve_strategy_config({}, MOMENTUM_STRATEGY_V2_ID)
        assert "ma_breakdown_max" in cfg["sell_signals"]

    def test_momentum_v3_adds_trend_score_max(self) -> None:
        cfg = resolve_strategy_config({}, MOMENTUM_STRATEGY_V3_ID)
        assert "trend_score_max" in cfg["buy_filters"]
        assert cfg.get("max_entry_trend_score") == 20.0

    def test_overrides_merge_correctly(self) -> None:
        cfg = resolve_strategy_config(self._trend_cfg(), TREND_STRATEGY_ID, overrides={"n_short": 7, "n_mid": 14})
        assert cfg["n_short"] == 7
        assert cfg["n_mid"] == 14

    def test_buy_filters_normalized(self) -> None:
        cfg = resolve_strategy_config({}, MOMENTUM_STRATEGY_ID, overrides={"buy_filters": "price_above_ma20|price_above_ma60"})
        assert "price_above_ma20" in cfg["buy_filters"]
        assert "price_above_ma60" in cfg["buy_filters"]


class TestBuildStrategyCatalog:
    def test_catalog_structure(self) -> None:
        catalog = build_strategy_catalog({})
        assert "default_id" in catalog
        assert "items" in catalog
        assert len(catalog["items"]) == 4  # trend + momentum v1/v2/v3
        ids = {item["id"] for item in catalog["items"]}
        assert ids == MOMENTUM_STRATEGY_IDS | {TREND_STRATEGY_ID}

    def test_catalog_has_params_by_strategy(self) -> None:
        catalog = build_strategy_catalog({})
        assert TREND_STRATEGY_ID in catalog["params_by_strategy"]
        assert MOMENTUM_STRATEGY_ID in catalog["params_by_strategy"]
