#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
sys.path.insert(0, str(PROJECT_ROOT))

from src.auto_resume_state import (  # noqa: E402
    leer_estado_job,
    job_debe_reanudarse,
    guardar_estado_job,
    marcar_job_completado,
    marcar_job_reintento,
)
from src.cobertura_runner import (  # noqa: E402
    ejecutar_coberturas_con_lock,
    ProcesoCoberturaYaEnEjecucion,
)
from src.oracle_jdbc import oracle_connect  # noqa: E402


def log(msg: str) -> None:
    print(msg, flush=True)


def contar_pendientes(
    username: str,
    password: str,
    fe_pla_aniomes_desde: str,
    dig_tramite: str = "",
) -> int:
    conn = None
    ps = None
    rs = None

    sql = """
        SELECT COUNT(*)
        FROM DIGITALIZACION.DIGITALIZACION
        WHERE TRIM(TO_CHAR(FE_PLA_ANIOMES)) >= ?
          AND NVL(TRIM(DIG_COBERTURA), 'N') = 'N'
          AND TRIM(DIG_PLANILLADO) = 'S'
    """

    params = [fe_pla_aniomes_desde]

    if dig_tramite:
        sql += " AND TO_CHAR(DIG_TRAMITE) = ?"
        params.append(dig_tramite)

    try:
        conn = oracle_connect(username, password)
        java_conn = conn.jconn

        ps = java_conn.prepareStatement(sql)

        for index, value in enumerate(params, start=1):
            ps.setString(index, str(value))

        ps.setQueryTimeout(60)
        rs = ps.executeQuery()

        if rs.next():
            return int(rs.getLong(1))

        return 0

    finally:
        if rs:
            try:
                rs.close()
            except Exception:
                pass

        if ps:
            try:
                ps.close()
            except Exception:
                pass

        if conn:
            try:
                conn.close()
            except Exception:
                pass


def ejecutar_sync_repo(output_dir: str, dig_tramite: str = "") -> None:
    script = PROJECT_ROOT / "scripts" / "sync_coberturas_repo.py"

    if not script.exists():
        log(f"[WARN] No existe script de sync: {script}")
        return

    cmd = [
        sys.executable,
        str(script),
        "--origen-root",
        output_dir,
        "--repo-root",
        "/data_nuevo/repo_grande/data/datos",
        "--logs-dir",
        str(PROJECT_ROOT / "logs"),
        "--state-db",
        str(PROJECT_ROOT / "logs" / "cobertura_repo_sync.sqlite"),
        "--apply",
    ]

    if dig_tramite:
        cmd.extend(["--tramite", dig_tramite])

    log("[INFO] Ejecutando sync al repositorio oficial...")
    completed = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=1800,
        check=False,
    )

    log(completed.stdout or "")

    if completed.returncode != 0:
        log(f"[WARN] Sync terminó con código {completed.returncode}")


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")

    import os

    if not job_debe_reanudarse():
        log("[INFO] No hay trabajo pendiente para reanudar.")
        return 0

    estado = leer_estado_job()

    fe_pla_aniomes_desde = str(estado.get("fe_pla_aniomes_desde", "")).strip()
    dig_tramite = str(estado.get("dig_tramite", "") or "").strip()
    output_dir = str(estado.get("output_dir", "/data_nuevo/coberturas")).strip()

    username = os.environ.get("ORACLE_AUTO_USER", "").strip()
    password = os.environ.get("ORACLE_AUTO_PASSWORD", "").strip()

    if not username or not password:
        error = "Faltan ORACLE_AUTO_USER u ORACLE_AUTO_PASSWORD en .env"
        marcar_job_reintento(error)
        log(f"[ERROR] {error}")
        return 1

    if not fe_pla_aniomes_desde:
        error = "No existe fe_pla_aniomes_desde en el estado de reanudación."
        marcar_job_reintento(error)
        log(f"[ERROR] {error}")
        return 1

    pendientes_antes = contar_pendientes(
        username=username,
        password=password,
        fe_pla_aniomes_desde=fe_pla_aniomes_desde,
        dig_tramite=dig_tramite,
    )

    log(f"[INFO] Pendientes antes de reanudar: {pendientes_antes}")

    if pendientes_antes <= 0:
        marcar_job_completado("No quedan pendientes.")
        ejecutar_sync_repo(output_dir=output_dir, dig_tramite=dig_tramite)
        log("[INFO] Trabajo completado. No quedan pendientes.")
        return 0

    guardar_estado_job(
        {
            "enabled": True,
            "status": "RUNNING_BY_WORKER",
            "last_error": "",
            "pendientes_antes": pendientes_antes,
        }
    )

    try:
        result = ejecutar_coberturas_con_lock(
            username=username,
            password=password,
            fe_pla_aniomes_desde=fe_pla_aniomes_desde,
            dig_tramite=dig_tramite,
            output_dir=output_dir,
            progress_callback=None,
        )

        log(f"[INFO] Resultado generación: {result}")

    except ProcesoCoberturaYaEnEjecucion as exc:
        guardar_estado_job(
            {
                "enabled": True,
                "status": "WAITING_OTHER_PROCESS",
                "last_error": str(exc),
            }
        )
        log(f"[INFO] {exc}")
        return 0

    except Exception as exc:
        marcar_job_reintento(str(exc))
        log(f"[ERROR] Falló reanudación: {exc}")
        return 1

    pendientes_despues = contar_pendientes(
        username=username,
        password=password,
        fe_pla_aniomes_desde=fe_pla_aniomes_desde,
        dig_tramite=dig_tramite,
    )

    log(f"[INFO] Pendientes después de ejecutar: {pendientes_despues}")

    ejecutar_sync_repo(output_dir=output_dir, dig_tramite=dig_tramite)

    if pendientes_despues <= 0:
        marcar_job_completado("Proceso terminado automáticamente.")
        log("[INFO] Trabajo completado.")
    else:
        guardar_estado_job(
            {
                "enabled": True,
                "status": "RETRY_PENDING",
                "pendientes_despues": pendientes_despues,
                "last_error": "",
            }
        )
        log("[INFO] Aún quedan pendientes. El timer volverá a ejecutar.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
