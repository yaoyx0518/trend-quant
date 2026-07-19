from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class AppSettings:
    name: str
    timezone: str
    host: str
    port: int
    data_provider_priority: list[str]
    update_time_after_close: str
    daily_update_max_retries: int
    daily_update_retry_interval_seconds: int


@dataclass(slots=True)
class LoggingSettings:
    level: str
    keep_forever: bool


@dataclass(slots=True)
class TickFlowSettings:
    """Limits for the currently subscribed TickFlow CN Starter plan."""

    plan: str
    api_base_url: str
    daily_kline_batch_size: int
    daily_kline_batch_requests_per_minute: int
    daily_kline_batch_max_workers: int
    daily_kline_single_requests_per_minute: int
    quote_max_symbols_per_request: int
    quote_requests_per_minute: int


@dataclass(slots=True)
class Settings:
    app: AppSettings
    tickflow: TickFlowSettings
    logging: LoggingSettings


DEFAULT_CONFIG_PATH = Path("config/app.yaml")


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_settings(config_path: Path | None = None) -> Settings:
    path = config_path or DEFAULT_CONFIG_PATH
    raw = _load_yaml(path)

    app_raw = raw.get("app", {})
    tickflow_raw = raw.get("tickflow", {})
    logging_raw = raw.get("logging", {})

    return Settings(
        app=AppSettings(
            name=str(app_raw.get("name", "trend-etf-system")),
            timezone=str(app_raw.get("timezone", "Asia/Shanghai")),
            host=str(app_raw.get("host", "127.0.0.1")),
            port=int(app_raw.get("port", 8000)),
            data_provider_priority=list(
                app_raw.get(
                    "data_provider_priority",
                    ["tickflow"],
                )
            ),
            update_time_after_close=str(app_raw.get("update_time_after_close", "16:30")),
            daily_update_max_retries=int(app_raw.get("daily_update_max_retries", 2)),
            daily_update_retry_interval_seconds=int(app_raw.get("daily_update_retry_interval_seconds", 5)),
        ),
        tickflow=TickFlowSettings(
            plan=str(tickflow_raw.get("plan", "starter")).strip().lower(),
            api_base_url=str(tickflow_raw.get("api_base_url", "https://api.tickflow.org")).strip(),
            daily_kline_batch_size=max(1, min(int(tickflow_raw.get("daily_kline_batch_size", 100)), 100)),
            daily_kline_batch_requests_per_minute=max(
                1,
                int(tickflow_raw.get("daily_kline_batch_requests_per_minute", 30)),
            ),
            daily_kline_batch_max_workers=max(
                1,
                int(tickflow_raw.get("daily_kline_batch_max_workers", 1)),
            ),
            daily_kline_single_requests_per_minute=max(
                1,
                int(tickflow_raw.get("daily_kline_single_requests_per_minute", 60)),
            ),
            quote_max_symbols_per_request=max(
                1,
                int(tickflow_raw.get("quote_max_symbols_per_request", 50)),
            ),
            quote_requests_per_minute=max(1, int(tickflow_raw.get("quote_requests_per_minute", 60))),
        ),
        logging=LoggingSettings(
            level=str(logging_raw.get("level", "INFO")),
            keep_forever=bool(logging_raw.get("keep_forever", True)),
        ),
    )
