from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class RunContext:
    run_id: str
    trigger: str
    started_at: datetime
