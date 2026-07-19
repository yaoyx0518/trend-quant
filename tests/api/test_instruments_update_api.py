"""API tests for the instrument category edit endpoint.

POST /instruments/api/{symbol}/update edits only the three-level categories
of a managed instrument. Symbol and name are not editable. The instrument
metadata table is the single source of truth; level priorities are
re-derived from the category registry.
"""

from __future__ import annotations

import pytest

CATEGORIES = [
    {"path": "股票", "level": 1, "name": "股票", "parent_path": "", "priority": 1},
    {"path": "股票-宽基", "level": 2, "name": "宽基", "parent_path": "股票", "priority": 1},
    {"path": "股票-宽基-沪深300", "level": 3, "name": "沪深300", "parent_path": "股票-宽基", "priority": 1},
    {"path": "商品", "level": 1, "name": "商品", "parent_path": "", "priority": 2},
    {"path": "商品-贵金属", "level": 2, "name": "贵金属", "parent_path": "商品", "priority": 1},
    {"path": "商品-贵金属-黄金", "level": 3, "name": "黄金", "parent_path": "商品-贵金属", "priority": 1},
]

SEED_METADATA = {
    "symbol": "510300.SS",
    "name": "沪深300ETF",
    "category_l1": "股票",
    "category_l2": "宽基",
    "category_l3": "沪深300",
    "factor_tags": ["大盘"],
    "region_tag": "",
    "priority_l1": 1,
    "priority_l2": 1,
    "priority_l3": 1,
    "sort_order": 1,
    "enabled": True,
    "stop_atr_mul": 1.5,
    "source": "test",
}


@pytest.fixture
def managed_instrument(test_db) -> None:
    """Seed one managed instrument in the metadata table."""
    test_db.save_instrument_categories(CATEGORIES)
    test_db.save_instrument_metadata([SEED_METADATA])


def test_update_categories_success(client, test_db, managed_instrument):
    resp = client.post(
        "/instruments/api/510300.SS/update",
        json={"category_l1": "商品", "category_l2": "贵金属", "category_l3": "黄金"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["category_path"] == "商品-贵金属-黄金"
    assert data["config_saved"] == 1
    assert data["metadata_saved"] == 1

    meta = test_db.get_instrument_metadata("510300.SS")
    assert meta["category_l1"] == "商品"
    assert meta["category_l2"] == "贵金属"
    assert meta["category_l3"] == "黄金"
    assert meta["priority_l1"] == 2
    assert meta["priority_l2"] == 1
    assert meta["priority_l3"] == 1
    assert meta["asset_type"] == "etf"
    # 其他属性保持不变
    assert meta["factor_tags"] == ["大盘"]
    assert meta["sort_order"] == 1
    assert meta["enabled"] == 1
    assert meta["stop_atr_mul"] == 1.5


def test_update_categories_keeps_symbol_and_name(client, test_db, managed_instrument):
    resp = client.post(
        "/instruments/api/510300.SS/update",
        json={
            "symbol": "999999.SS",
            "name": "试图改名",
            "category_l1": "商品",
            "category_l2": "贵金属",
            "category_l3": "黄金",
        },
    )
    assert resp.status_code == 200
    meta = test_db.get_instrument_metadata("510300.SS")
    assert meta["name"] == "沪深300ETF"
    assert test_db.get_instrument_metadata("999999.SS") is None


def test_update_categories_invalid_combination(client, managed_instrument):
    resp = client.post(
        "/instruments/api/510300.SS/update",
        json={"category_l1": "商品", "category_l2": "宽基", "category_l3": "沪深300"},
    )
    assert resp.status_code == 400
    assert "类目组合不存在" in resp.json()["detail"]


def test_update_categories_missing_level(client, managed_instrument):
    resp = client.post(
        "/instruments/api/510300.SS/update",
        json={"category_l1": "商品", "category_l2": "", "category_l3": "黄金"},
    )
    assert resp.status_code == 400
    assert "一二三级类目均必选" in resp.json()["detail"]


def test_update_unknown_symbol_returns_404(client, managed_instrument):
    resp = client.post(
        "/instruments/api/999999.SS/update",
        json={"category_l1": "商品", "category_l2": "贵金属", "category_l3": "黄金"},
    )
    assert resp.status_code == 404
