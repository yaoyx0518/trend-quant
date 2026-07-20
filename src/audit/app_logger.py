from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path("logs/app")
APP_LOG_PATH = LOG_DIR / "app.log"
ACCESS_LOG_PATH = LOG_DIR / "access.log"

_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"

# app.log rotates at 10MB x 5 backups (~3 weeks at current volume).
APP_LOG_MAX_BYTES = 10 * 1024 * 1024
APP_LOG_BACKUP_COUNT = 5
# access.log keeps the HTTP request timeline (uvicorn.access) in a separate
# file so request-level investigations do not require journalctl.
ACCESS_LOG_MAX_BYTES = 10 * 1024 * 1024
ACCESS_LOG_BACKUP_COUNT = 3


def setup_logging(level: str = "INFO") -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=_LOG_FORMAT,
        handlers=[
            RotatingFileHandler(
                APP_LOG_PATH,
                maxBytes=APP_LOG_MAX_BYTES,
                backupCount=APP_LOG_BACKUP_COUNT,
                encoding="utf-8",
            ),
            logging.StreamHandler(),
        ],
    )
    _attach_access_log_handler()


def _attach_access_log_handler() -> None:
    # uvicorn.access uses its own stdout handler with propagate=False, so the
    # request timeline never reaches the root handlers; attach a dedicated
    # rotating file handler to persist it to logs/app/access.log.
    access_logger = logging.getLogger("uvicorn.access")
    if any(isinstance(h, RotatingFileHandler) for h in access_logger.handlers):
        return
    handler = RotatingFileHandler(
        ACCESS_LOG_PATH,
        maxBytes=ACCESS_LOG_MAX_BYTES,
        backupCount=ACCESS_LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    access_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
