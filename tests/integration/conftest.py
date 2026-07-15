"""Integration‑test layer fixtures.

Key responsibility: override the global ``get_db()`` singleton so that
every integration test uses its own isolated database, never the
production ``data/trend_quant.db``.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def isolate_get_db(test_db, monkeypatch: pytest.MonkeyPatch) -> None:
    """Redirect ``data.storage.db.get_db()`` → *test_db*.

    This is ``autouse=True`` so every integration test is automatically
    shielded from the production database.
    """
    import data.storage.db as db_module

    monkeypatch.setattr(db_module, "get_db", lambda: test_db)
    monkeypatch.setattr(db_module, "_db_instance", test_db)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        item.add_marker(pytest.mark.integration)
