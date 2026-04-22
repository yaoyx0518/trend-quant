from __future__ import annotations

from typing import Protocol


class INotifier(Protocol):
    channel: str

    def send(self, level: str, title: str, content: str, context: dict | None = None) -> bool: ...
