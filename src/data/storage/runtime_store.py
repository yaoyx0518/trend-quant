from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class RuntimeStore:
    def __init__(self, base_dir: str = "data/runtime") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def write_json(self, relative_path: str, payload: Any) -> Path:
        path = self.base_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return path

    def read_json(self, relative_path: str, default: Any = None) -> Any:
        path = self.base_dir / relative_path
        if not path.exists():
            return default
        with path.open("r", encoding="utf-8-sig") as f:
            return json.load(f)
