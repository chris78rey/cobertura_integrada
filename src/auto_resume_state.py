from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
STATE_PATH = PROJECT_ROOT / "logs" / "cobertura_auto_resume_state.json"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
    tmp_path.write_text(
        json.dumps(actual, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(tmp_path, STATE_PATH)


def registrar_job_activo(
    fe_pla_aniomes_desde: str,
    output_dir: str,
    dig_tramite: str = "",
) -> None:
    guardar_estado_job(
        {
            "enabled": True,
            "status": "RUNNING",
            "fe_pla_aniomes_desde": str(fe_pla_aniomes_desde).strip(),
            "dig_tramite": str(dig_tramite or "").strip(),
            "output_dir": str(output_dir).strip(),
            "started_at": _now(),
            "completed_at": "",
            "last_error": "",
        }
    )


def marcar_job_completado(detalle: str = "") -> None:
    guardar_estado_job(
        {
            "enabled": False,
            "status": "COMPLETED",
            "completed_at": _now(),
            "last_error": "",
            "detalle": detalle,
        }
    )


def marcar_job_reintento(error: str) -> None:
    guardar_estado_job(
        {
            "enabled": True,
            "status": "RETRY_PENDING",
            "last_error": str(error),
        }
    )


def marcar_job_detenido_por_usuario() -> None:
    guardar_estado_job(
        {
            "enabled": False,
            "status": "STOPPED_BY_USER",
            "last_error": "",
            "detalle": "Proceso detenido manualmente desde Streamlit.",
        }
    )


def job_debe_reanudarse() -> bool:
    estado = leer_estado_job()

    if not estado:
        return False

    if not estado.get("enabled"):
        return False

    return estado.get("status") in {
        "RUNNING",
        "RETRY_PENDING",
        "RUNNING_BY_WORKER",
        "WAITING_OTHER_PROCESS",
    }
