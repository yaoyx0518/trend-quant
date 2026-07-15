"""Unit‑test layer markers — all tests in this directory are automatically
tagged with ``@pytest.mark.unit``.
"""

import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        item.add_marker(pytest.mark.unit)
