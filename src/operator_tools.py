from __future__ import annotations

import fcntl
import json
import os
import time
from pathlib import Path
from typing import Any

from src.auto_resume_state import leer_estado_job, guardar_estado_job
from src.quarantine import (
    contar_en_cuarentena,
    listar_cuarentena_activa,
    expirar_cuarentena_activa,
)


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
OUTPUT_ROOT = Path("/data_nuevo/coberturas")

LOG_DIR = PROJECT_ROOT / "logs"
GEN_LOCK = LOG_DIR / "cobertura_generation.lock"
SYNC_LOCK = LOG_DIR / "cobertura_repo_sync.lock"
STOP_FLAG = PROJECT_ROOT / "config" / "stop_cobertura.flag"


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _leer_texto_seguro(path: Path, max_chars: int = 2000) -> str:
    try:
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8", errors="replace")[:max_chars]
    except Exception as exc:
        return f"No se pudo leer {path}: {exc}"


def _lock_status(path: Path) -> dict[str, Any]:
    status = {
        "path": str(path),
        "exists": path.exists(),
        "held": False,
        "orphan": False,
        "content": "",
    }
    if not path.exists():
        return status
    status["content"] = _leer_texto_seguro(path, max_chars=500)
    fh = None
    try:
        fh = path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            status["held"] = False
            status["orphan"] = True
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except BlockingIOError:
            status["held"] = True
            status["orphan"] = False
    except Exception as exc:
        status["held"] = True
        status["orphan"] = False
        status["content"] = f"{status['content']}\nError: {exc}"
    finally:
        if fh:
            try:
                fh.close()
            except Exception:
                pass
    return status


def limpiar_locks_huerfanos() -> dict[str, Any]:
    resultado = {"deleted": [], "kept_active": [], "errors": []}
    for path in [GEN_LOCK, SYNC_LOCK]:
        status = _lock_status(path)
        if not status["exists"]:
            continue
        if status["held"]:
            resultado["kept_active"].append(str(path))
            continue
        if status["orphan"]:
            try:
                path.unlink()
                resultado["deleted"].append(str(path))
            except Exception as exc:
                resultado["errors"].append(f"{path}: {exc}")
    return resultado


def limpiar_bandera_parada() -> bool:
    if STOP_FLAG.exists():
        STOP_FLAG.unlink()
        return True
    return False


def leer_ultimos_errores(max_lineas: int = 80) -> list[str]:
    logs = sorted(LOG_DIR.glob("cobertura_auto_*.jsonl"), key=lambda p: p.stat().st_mtime)
    if not logs:
        return []
    ultimo = logs[-1]
    lineas = []
    try:
        with ultimo.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if any(t in line for t in [
                    "ERROR", "PDF_GENERATION_ERROR", "ORACLE_UPDATE_ERROR",
                    "DB_ONLY_QUARANTINED_OR_EXCLUDED", "PDF_ALREADY_EXISTS",
                ]):
                    lineas.append(line.strip())
    except Exception as exc:
        return [f"No se pudo leer log {ultimo}: {exc}"]
    return lineas[-max_lineas:]


def leer_estado_operador() -> dict[str, Any]:
    gen_lock = _lock_status(GEN_LOCK)
    sync_lock = _lock_status(SYNC_LOCK)
    estado_job = leer_estado_job()
    cuarentena = listar_cuarentena_activa(limit=30)
    return {
        "timestamp": _now(),
        "streamlit_ok": True,
        "output_root_exists": OUTPUT_ROOT.exists(),
        "stop_flag": STOP_FLAG.exists(),
        "generation_lock": gen_lock,
        "sync_lock": sync_lock,
        "job": estado_job,
        "quarantine_count": contar_en_cuarentena(),
        "quarantine_rows": cuarentena,
        "last_errors": leer_ultimos_errores(max_lineas=40),
    }


def habilitar_reintento_automatico() -> None:
    estado_actual = leer_estado_job()
    guardar_estado_job({
        "enabled": True,
        "status": "RETRY_PENDING",
        "retry_count": 0,
        "last_error": "",
        "detalle": "Reintento habilitado desde panel de operación segura.",
        "fe_pla_aniomes_desde": str(estado_actual.get("fe_pla_aniomes_desde", "")).strip(),
        "dig_tramite": str(estado_actual.get("dig_tramite", "")).strip(),
        "output_dir": str(estado_actual.get("output_dir", str(OUTPUT_ROOT))).strip(),
    })


def pausar_reintento_automatico() -> None:
    guardar_estado_job({
        "enabled": False,
        "status": "PAUSED_BY_OPERATOR",
        "last_error": "",
        "detalle": "Reintento automático pausado desde panel de operación segura.",
    })


def destrabar_para_reintento() -> dict[str, Any]:
    resultado: dict[str, Any] = {
        "ok": True,
        "quarantine_expired": 0,
        "stop_flag_removed": False,
        "locks": {},
        "retry_enabled": False,
        "warnings": [],
    }
    resultado["quarantine_expired"] = expirar_cuarentena_activa()
    resultado["stop_flag_removed"] = limpiar_bandera_parada()
    resultado["locks"] = limpiar_locks_huerfanos()

    estado_despues = leer_estado_operador()
    hay_lock_activo = bool(
        estado_despues["generation_lock"]["held"]
        or estado_despues["sync_lock"]["held"]
    )
    if hay_lock_activo:
        resultado["ok"] = False
        resultado["warnings"].append(
            "Hay un proceso activo. No se habilitó reintento para evitar doble ejecución."
        )
        return resultado

    habilitar_reintento_automatico()
    resultado["retry_enabled"] = True
    return resultado


def exportar_estado_json() -> str:
    estado = leer_estado_operador()
    return json.dumps(estado, ensure_ascii=False, indent=2, default=str)
