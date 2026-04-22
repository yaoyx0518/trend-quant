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
    polling_times: list[str]
    final_signal_time: str
    update_time_after_close: str
    market_fetch_retry_times: int
    market_fetch_retry_interval_seconds: int
    notify_retry_times: int
    notify_retry_interval_seconds: int
    lot_size: int


@dataclass(slots=True)
class RuntimeSettings:
    account_equity_default: float
    ensure_dirs: bool


@dataclass(slots=True)
class LoggingSettings:
    level: str
    keep_forever: bool


@dataclass(slots=True)
class Settings:
    app: AppSettings
    runtime: RuntimeSettings
    logging: LoggingSettings


DEFAULT_CONFIG_PATH = Path("config/app.yaml")


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_settings(config_path: Path | None = None) -> Settings:
    path = config_path or DEFAULT_CONFIG_PATH
    raw = _load_yaml(path)

    app_raw = raw.get("app", {})
    runtime_raw = raw.get("runtime", {})
    logging_raw = raw.get("logging", {})

    return Settings(
        app=AppSettings(
            name=str(app_raw.get("name", "trend-etf-system")),
            timezone=str(app_raw.get("timezone", "Asia/Shanghai")),
            host=str(app_raw.get("host", "127.0.0.1")),
            port=int(app_raw.get("port", 8000)),
            data_provider_priority=list(app_raw.get("data_provider_priority", ["efinance", "akshare"])),
            polling_times=list(app_raw.get("polling_times", [])),
            final_signal_time=str(app_raw.get("final_signal_time", "14:45")),
            update_time_after_close=str(app_raw.get("update_time_after_close", "15:30")),
            market_fetch_retry_times=int(app_raw.get("market_fetch_retry_times", 3)),
            market_fetch_retry_interval_seconds=int(app_raw.get("market_fetch_retry_interval_seconds", 20)),
            notify_retry_times=int(app_raw.get("notify_retry_times", 2)),
            notify_retry_interval_seconds=int(app_raw.get("notify_retry_interval_seconds", 5)),
            lot_size=int(app_raw.get("lot_size", 100)),
        ),
        runtime=RuntimeSettings(
            account_equity_default=float(runtime_raw.get("account_equity_default", 200000)),
            ensure_dirs=bool(runtime_raw.get("ensure_dirs", True)),
        ),
        logging=LoggingSettings(
            level=str(logging_raw.get("level", "INFO")),
            keep_forever=bool(logging_raw.get("keep_forever", True)),
        ),
    )
