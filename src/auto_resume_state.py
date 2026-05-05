from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
STATE_PATH = PROJECT_ROOT / "logs" / "cobertura_auto_resume_state.json"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _watch_interval_seconds() -> int:
    raw = os.environ.get("COBERTURA_WATCH_INTERVAL_SECONDS", "60").strip()
    try:
        value = int(raw)
    except ValueError:
        value = 60
    return max(30, min(value, 900))


def _sync_queue_db() -> sqlite3.Connection:
    db_path = PROJECT_ROOT / "logs" / "cobertura_sync_queue.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cobertura_sync_queue (
            tramite TEXT PRIMARY KEY,
            source_dir TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            synced_at TEXT DEFAULT ''
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sync_queue_status ON cobertura_sync_queue(status)")
    conn.commit()
    return conn


def marcar_tramite_sync_pendiente(tramite: str, source_dir: str, detalle: str = "") -> None:
    now = _now()
    conn = _sync_queue_db()
    conn.execute("""
        INSERT INTO cobertura_sync_queue (tramite, source_dir, status, attempts, created_at, updated_at)
        VALUES (?, ?, 'PENDING', 0, ?, ?)
        ON CONFLICT(tramite) DO UPDATE SET status='PENDING', attempts=0, last_error='', updated_at=?
    """, (tramite, source_dir, now, now, now))
    conn.commit()
    conn.close()


def obtener_tramites_sync_pendientes(limit: int = 500) -> list[dict[str, Any]]:
    conn = _sync_queue_db()
    rows = conn.execute(
        "SELECT tramite, source_dir, attempts FROM cobertura_sync_queue WHERE status='PENDING' ORDER BY created_at LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [{"tramite": r[0], "source_dir": r[1], "attempts": r[2]} for r in rows]


def marcar_tramite_sync_ok(tramite: str) -> None:
    now = _now()
    conn = _sync_queue_db()
    conn.execute("UPDATE cobertura_sync_queue SET status='SYNC_OK', synced_at=?, updated_at=? WHERE tramite=?",
                 (now, now, tramite))
    conn.commit()
    conn.close()


def marcar_tramite_sync_error(tramite: str, error: str) -> None:
    now = _now()
    conn = _sync_queue_db()
    conn.execute("UPDATE cobertura_sync_queue SET attempts=attempts+1, last_error=?, updated_at=? WHERE tramite=?",
                 (str(error)[:500], now, tramite))
    conn.commit()
    conn.close()


def resumen_cola_sync() -> dict[str, int]:
    conn = _sync_queue_db()
    rows = conn.execute("SELECT status, COUNT(*) FROM cobertura_sync_queue GROUP BY status").fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


def leer_estado_job() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def guardar_estado_job(data: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    actual = leer_estado_job()
    actual.update(data)
    actual["updated_at"] = _now()
    tmp_path = STATE_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(actual, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp_path, STATE_PATH)


def registrar_job_activo(fe_pla_aniomes_desde: str, output_dir: str, dig_tramite: str = "") -> None:
    guardar_estado_job({
        "enabled": True, "status": "RUNNING",
        "fe_pla_aniomes_desde": str(fe_pla_aniomes_desde).strip(),
        "dig_tramite": str(dig_tramite or "").strip(),
        "output_dir": str(output_dir).strip(),
        "started_at": _now(), "completed_at": "", "last_error": "",
        "retry_count": 0, "watch_empty_cycles": 0, "sync_pending": False,
        "detalle": "Proceso activo.",
    })


def marcar_job_vigilando_sin_pendientes(detalle: str = "", sync_pending: bool | None = None) -> None:
    estado = leer_estado_job()
    ciclos = int(estado.get("watch_empty_cycles", 0)) + 1
    if sync_pending is None:
        sync_pending_final = bool(estado.get("sync_pending", False))
    else:
        sync_pending_final = bool(sync_pending)
    guardar_estado_job({
        "enabled": True, "status": "WATCHING_NO_PENDING",
        "completed_at": "", "last_error": "", "retry_count": 0,
        "watch_empty_cycles": ciclos, "last_watch_at": _now(),
        "sync_pending": sync_pending_final,
        "detalle": detalle or "No hay pendientes. El sistema sigue vigilando Oracle.",
    })


def marcar_job_completado(detalle: str = "") -> None:
    guardar_estado_job({
        "enabled": False, "status": "COMPLETED",
        "completed_at": _now(), "last_error": "", "retry_count": 0, "sync_pending": False,
        "detalle": detalle,
    })


def marcar_job_reintento(error: str) -> None:
    estado = leer_estado_job()
    retries = int(estado.get("retry_count", 0)) + 1
    if retries >= 5:
        guardar_estado_job({
            "enabled": True, "status": "RETRY_PENDING_SLOW",
            "last_error": str(error), "retry_count": retries,
            "detalle": "5 reintentos. Se mantiene en reintento lento.",
        })
    else:
        guardar_estado_job({
            "enabled": True, "status": "RETRY_PENDING",
            "last_error": str(error), "retry_count": retries,
        })


def marcar_job_detenido_por_usuario() -> None:
    guardar_estado_job({
        "enabled": False, "status": "STOPPED_BY_USER",
        "last_error": "", "detalle": "Proceso detenido manualmente desde Streamlit.",
    })


def job_debe_reanudarse() -> bool:
    estado = leer_estado_job()
    if not estado:
        return False
    if not estado.get("enabled"):
        return False
    status = str(estado.get("status", "")).strip()
    if status not in {
        "RUNNING", "RETRY_PENDING", "RETRY_PENDING_SLOW",
        "RUNNING_BY_WORKER", "WAITING_OTHER_PROCESS", "WATCHING_NO_PENDING",
    }:
        return False
    updated_at = estado.get("updated_at", "")
    if updated_at:
        try:
            ultima = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
            segundos = (datetime.now() - ultima).total_seconds()
            if status in ("RUNNING", "RUNNING_BY_WORKER") and segundos < 60:
                return False
            if status == "RETRY_PENDING" and segundos < 60:
                return False
            if status == "RETRY_PENDING_SLOW" and segundos < 600:
                return False
            if status == "WAITING_OTHER_PROCESS" and segundos < 120:
                return False
            if status == "WATCHING_NO_PENDING" and segundos < _watch_interval_seconds():
                return False
        except ValueError:
            pass
    return True
