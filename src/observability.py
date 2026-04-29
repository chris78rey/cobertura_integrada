# =========================
# src/observability.py
# Observabilidad simple para procesos largos de cobertura
# =========================

from __future__ import annotations

import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any


LOG_DIR = Path(__file__).resolve().parent.parent / "logs"


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_log_dir() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    return LOG_DIR


def mask_cedula(value: str | None) -> str:
    raw = str(value or "").strip()
    if len(raw) <= 4:
        return raw
    return f"{raw[:3]}****{raw[-3:]}"


def build_run_id(prefix: str = "cobertura_auto") -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{stamp}"


class RunLogger:
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.log_dir = ensure_log_dir()
        self.jsonl_path = self.log_dir / f"{run_id}.jsonl"
        self.error_path = self.log_dir / f"{run_id}_errors.jsonl"

    def event(self, event: str, **data: Any) -> None:
        payload = {
            "ts": now_iso(),
            "run_id": self.run_id,
            "event": event,
            **data,
        }
        with self.jsonl_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")

    def error(self, event: str, exc: Exception | str, **data: Any) -> None:
        if isinstance(exc, Exception):
            error_message = str(exc)
            error_type = exc.__class__.__name__
            trace = traceback.format_exc(limit=8)
        else:
            error_message = str(exc)
            error_type = "ERROR"
            trace = ""

        payload = {
            "ts": now_iso(),
            "run_id": self.run_id,
            "event": event,
            "error_type": error_type,
            "error": error_message,
            "traceback": trace,
            **data,
        }
        with self.error_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        self.event(event, **payload)

    def paths(self) -> dict[str, str]:
        return {
            "run_log_path": str(self.jsonl_path),
            "error_log_path": str(self.error_path),
        }
