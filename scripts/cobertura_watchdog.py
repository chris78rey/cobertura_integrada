#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
sys.path.insert(0, str(PROJECT_ROOT))

from src.auto_resume_state import _parse_ts


STATE_PATH = PROJECT_ROOT / "logs" / "cobertura_auto_resume_state.json"
WATCHDOG_STATE_PATH = PROJECT_ROOT / "logs" / "cobertura_watchdog_state.json"
WATCHDOG_LOG_PATH = PROJECT_ROOT / "logs" / "cobertura_watchdog.log"
SERVICE_NAME = "cobertura-auto-resume.service"
TIMER_NAME = "cobertura-auto-resume.timer"
DEFAULT_STALE_MINUTES = 20
DEFAULT_COOLDOWN_MINUTES = 30


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    WATCHDOG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with WATCHDOG_LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _age_seconds(value: str) -> float | None:
    dt = _parse_ts(value)
    if dt is None:
        return None
    return (datetime.now(timezone.utc) - dt).total_seconds()


def _systemctl_is_active(unit: str) -> str:
    completed = subprocess.run(
        ["systemctl", "is-active", unit],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return (completed.stdout or "").strip()


def _systemctl_restart(unit: str) -> None:
    subprocess.run(["systemctl", "restart", unit], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _systemctl_start(unit: str) -> None:
    subprocess.run(["systemctl", "start", unit], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _systemctl_show_main_pid(unit: str) -> int:
    completed = subprocess.run(
        ["systemctl", "show", unit, "-p", "MainPID", "--value"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    raw = (completed.stdout or "").strip()
    try:
        return int(raw or "0")
    except ValueError:
        return 0


def _ps_elapsed_seconds(pid: int) -> int | None:
    if pid <= 0:
        return None
    completed = subprocess.run(
        ["ps", "-o", "etimes=", "-p", str(pid)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    raw = (completed.stdout or "").strip()
    try:
        return int(raw)
    except ValueError:
        return None


def _last_service_log_age_seconds(unit: str) -> float | None:
    completed = subprocess.run(
        ["journalctl", "-u", unit, "-n", "1", "-o", "short-iso", "--no-pager"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    line = (completed.stdout or "").strip().splitlines()
    if not line:
        return None
    first = line[-1].split(" ", 1)[0].strip()
    try:
        dt = datetime.fromisoformat(first)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds()


def _sync_child_info() -> dict[str, Any]:
    completed = subprocess.run(
        ["ps", "-eo", "pid=,ppid=,etimes=,cmd="],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    info: dict[str, Any] = {"pid": 0, "ppid": 0, "elapsed": None, "cmd": ""}
    for line in (completed.stdout or "").splitlines():
        stripped = line.strip()
        if "scripts/sync_coberturas_repo.py" not in stripped:
            continue
        if "--origen-root /data_nuevo/coberturas" not in stripped:
            continue
        parts = stripped.split(maxsplit=3)
        if len(parts) < 4:
            continue
        try:
            info = {
                "pid": int(parts[0]),
                "ppid": int(parts[1]),
                "elapsed": int(parts[2]),
                "cmd": parts[3],
            }
            return info
        except ValueError:
            continue
    return info


@dataclass
class Verdict:
    action: str
    reason: str


def main() -> int:
    stale_minutes = int(os.environ.get("COBERTURA_WATCHDOG_STALE_MINUTES", str(DEFAULT_STALE_MINUTES)))
    cooldown_minutes = int(os.environ.get("COBERTURA_WATCHDOG_COOLDOWN_MINUTES", str(DEFAULT_COOLDOWN_MINUTES)))
    stale_seconds = max(300, stale_minutes * 60)
    cooldown_seconds = max(300, cooldown_minutes * 60)

    state = _load_json(STATE_PATH)
    watchdog_state = _load_json(WATCHDOG_STATE_PATH)
    now = datetime.now(timezone.utc)
    last_restart_at = _parse_ts(str(watchdog_state.get("last_restart_at", "")))
    restart_age = (now - last_restart_at).total_seconds() if last_restart_at else None

    service_active = _systemctl_is_active(SERVICE_NAME)
    timer_active = _systemctl_is_active(TIMER_NAME)
    main_pid = _systemctl_show_main_pid(SERVICE_NAME)
    main_pid_age = _ps_elapsed_seconds(main_pid)
    last_log_age = _last_service_log_age_seconds(SERVICE_NAME)
    sync_info = _sync_child_info()
    sync_age = sync_info.get("elapsed")

    sync_active = bool(state.get("sync_active"))
    sync_since_age = _age_seconds(str(state.get("sync_active_since", "")))
    progress_age = _age_seconds(str(state.get("last_progress_at", "")))
    status = str(state.get("status", "")).strip()
    enabled = bool(state.get("enabled"))

    verdict = Verdict(action="noop", reason="Sin indicios de estancamiento.")

    if enabled and service_active == "inactive" and timer_active == "active":
        _systemctl_start(SERVICE_NAME)
        verdict = Verdict(action="start", reason="Servicio inactivo con timer activo y reanudación habilitada.")

    if enabled and service_active in {"active", "activating"}:
        if sync_active:
            verdict = Verdict(action="noop", reason="Sync activo: no se reinicia por watchdog.")
        elif status in {"WATCHING_NO_PENDING", "WAITING_OTHER_PROCESS"}:
            verdict = Verdict(action="noop", reason=f"Estado {status}: no se reinicia por watchdog.")
        elif status in {"RUNNING", "RUNNING_BY_WORKER", "SYNC_ACTIVE"}:
            candidates: list[float] = []
            for value in (last_log_age, progress_age):
                if value is not None:
                    candidates.append(float(value))
            if main_pid_age is not None:
                candidates.append(float(main_pid_age))
            if candidates:
                oldest = max(candidates)
                if oldest > stale_seconds and (restart_age is None or restart_age > cooldown_seconds):
                    _systemctl_restart(SERVICE_NAME)
                    verdict = Verdict(
                        action="restart",
                        reason=(
                            f"Estado {status} sin actividad reciente por {int(oldest // 60)} min. "
                            f"PID={main_pid}, sync_pid={sync_info.get('pid', 0)}."
                        ),
                    )

    snapshot = {
        "checked_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "service_active": service_active,
        "timer_active": timer_active,
        "main_pid": main_pid,
        "main_pid_age_seconds": main_pid_age,
        "last_log_age_seconds": last_log_age,
        "sync_pid": sync_info.get("pid", 0),
        "sync_pid_age_seconds": sync_age,
        "status": status,
        "enabled": enabled,
        "sync_active": sync_active,
        "sync_active_tramite": state.get("sync_active_tramite", ""),
        "sync_active_since": state.get("sync_active_since", ""),
        "last_progress_at": state.get("last_progress_at", ""),
        "verdict": verdict.__dict__,
        "last_restart_at": watchdog_state.get("last_restart_at", ""),
    }
    if verdict.action == "restart":
        snapshot["last_restart_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        _save_json(WATCHDOG_STATE_PATH, snapshot)
        log(f"RESTART: {verdict.reason}")
        return 0

    _save_json(WATCHDOG_STATE_PATH, snapshot)
    log(f"OK: {verdict.reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
