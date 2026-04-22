from __future__ import annotations

import json
from pathlib import Path


class CalcLogger:
    def __init__(self, file_path: str = "logs/calc/calc.jsonl") -> None:
        self.path = Path(file_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, payload: dict) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
