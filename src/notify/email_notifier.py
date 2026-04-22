from __future__ import annotations

from audit.app_logger import get_logger

logger = get_logger(__name__)


class EmailNotifier:
    channel = "email"

    def send(self, level: str, title: str, content: str, context: dict | None = None) -> bool:
        logger.info("[email][%s] %s | %s", level, title, content)
        return True
