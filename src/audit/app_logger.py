from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(level: str = "INFO") -> None:
    Path("logs/app").mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[
            logging.FileHandler("logs/app/app.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
