#!/usr/bin/env python3
from __future__ import annotations

import time
import os
import sys
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
sys.path.insert(0, str(PROJECT_ROOT))

from src.auto_resume_state import (  # noqa: E402
    leer_estado_job,
    guardar_estado_job,
    heartbeat_job,
    marcar_job_reintento,
    marcar_job_vigilando_sin_pendientes,
)
from src.cobertura_runner import (  # noqa: E402
    ejecutar_coberturas_con_lock,
    ProcesoCoberturaYaEnEjecucion,
)
from src.oracle_jdbc import oracle_connect  # noqa: E402
from src.oracle_jdbc import actualizar_cobertura_por_tramite_valor  # noqa: E402


def log(msg: str) -> None:
    print(msg, flush=True)


def _limpiar_logs_antiguos() -> None:
    logs_dir = PROJECT_ROOT / "logs"
    ahora = __import__("time").time()
    dia = 86400
    for f in logs_dir.glob("cobertura_auto_*.jsonl"):
        try:
            if ahora - f.stat().st_mtime > 30 * dia:
                f.unlink()
        except Exception:
            pass
    for f in logs_dir.glob("cobertura_repo_sync_*.csv"):
        try:
            if ahora - f.stat().st_mtime > 90 * dia:
                f.unlink()
        except Exception:
            pass


def _es_modo_vigilante(dig_tramite: str) -> bool:
    return not str(dig_tramite or "").strip()


def contar_pendientes(username: str, password: str, fe_pla_aniomes_desde: str, dig_tramite: str = "") -> int:
    conn = None
    ps = None
    rs = None
    sql = """SELECT COUNT(*) FROM DIGITALIZACION.DIGITALIZACION
        WHERE TRIM(TO_CHAR(FE_PLA_ANIOMES)) >= ? AND NVL(TRIM(DIG_COBERTURA),'N')='N' AND TRIM(DIG_PLANILLADO)='S'"""
    params = [fe_pla_aniomes_desde]
    if dig_tramite:
        sql += " AND TO_CHAR(DIG_TRAMITE) = ?"
        params.append(dig_tramite)
    try:
        conn = oracle_connect(username, password)
        ps = conn.jconn.prepareStatement(sql)
        for i, v in enumerate(params, start=1):
            ps.setString(i, str(v))
        ps.setQueryTimeout(60)
        rs = ps.executeQuery()
        if rs.next():
            return int(rs.getLong(1))
        return 0
    finally:
        for obj in (rs, ps, conn):
            if obj:
                try: obj.close()
                except Exception: pass

def _marcar_tramite_x(username: str, password: str, tramite: str, motivo: str) -> None:
    tramite = str(tramite or "").strip()
    if not tramite:
        return

    try:
        res = actualizar_cobertura_por_tramite_valor(username, password, tramite, "X")
        log(
            f"[INFO] Trámite {tramite} marcado en X "
            f"(ok={res.get('ok')}, affected={res.get('affected', 0)}). Motivo: {motivo}"
        )
    except Exception as exc:
        log(f"[ERROR] No se pudo marcar trámite {tramite} en X: {exc}")


def _run_cycle() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    import os
    _limpiar_logs_antiguos()

    estado = leer_estado_job()
    fe_pla_aniomes_desde = str(estado.get("fe_pla_aniomes_desde", "")).strip()
    dig_tramite = str(estado.get("dig_tramite", "") or "").strip()
    output_dir = "/data_nuevo/coberturas"
    username = os.environ.get("ORACLE_AUTO_USER", "").strip()
    password = os.environ.get("ORACLE_AUTO_PASSWORD", "").strip()

    if not username or not password:
        marcar_job_reintento("Faltan ORACLE_AUTO_USER u ORACLE_AUTO_PASSWORD en .env")
        return 1
    if not fe_pla_aniomes_desde:
        marcar_job_reintento("No existe fe_pla_aniomes_desde en el estado.")
        return 1

    modo_vigilante = _es_modo_vigilante(dig_tramite)
    pendientes_antes = contar_pendientes(username, password, fe_pla_aniomes_desde, dig_tramite)
    log(f"[INFO] Pendientes antes de ejecutar: {pendientes_antes}")

    heartbeat_job(
        enabled=True,
        status="RUNNING_BY_WORKER" if pendientes_antes > 0 else "WATCHING_NO_PENDING",
        pendientes_antes=pendientes_antes,
        detalle=(
            f"Worker revisando Oracle para FE_PLA_ANIOMES >= {fe_pla_aniomes_desde}"
            + (f" y trámite {dig_tramite}" if dig_tramite else "")
        ),
    )

    if pendientes_antes <= 0:
        if modo_vigilante:
            marcar_job_vigilando_sin_pendientes(
                f"No hay pendientes con FE_PLA_ANIOMES >= {fe_pla_aniomes_desde}. Sistema vigilando.",
                sync_pending=False)
            log("[INFO] Sin pendientes. El loop sigue vigilando.")
        return 0

        guardar_estado_job({"enabled": True, "status": "RUNNING_BY_WORKER",
                        "last_error": "", "retry_count": 0,
                        "pendientes_antes": pendientes_antes,
                        "pendientes_despues": "",
                        "last_generados": "",
                        "last_actualizados": "",
                        "last_errores": "",
                        "detalle": (
                            f"Worker revisando Oracle para FE_PLA_ANIOMES >= {fe_pla_aniomes_desde}"
                            + (f" y trámite {dig_tramite}" if dig_tramite else "")
                        ),
                        })

    try:
        heartbeat_job(
            enabled=True,
            status="RUNNING_BY_WORKER",
            sync_active=False,
            detalle="Generación de coberturas en ejecución.",
        )
        result = ejecutar_coberturas_con_lock(
            username=username, password=password, fe_pla_aniomes_desde=fe_pla_aniomes_desde,
            dig_tramite=dig_tramite, output_dir=output_dir, progress_callback=None)
        log(f"[INFO] Resultado generación: generados={result.get('generados',0)}, actualizados={result.get('actualizados',0)}, errores={result.get('errores',0)}")
        heartbeat_job(
            enabled=True,
            status="RUNNING_BY_WORKER",
            last_generados=result.get("generados", 0),
            last_actualizados=result.get("actualizados", 0),
            last_errores=result.get("errores", 0),
            last_run_id=result.get("run_id", ""),
            last_manifest_path=result.get("manifest_path", ""),
            detalle=(
                f"Generación terminada. "
                f"Generados={result.get('generados', 0)}, "
                f"actualizados={result.get('actualizados', 0)}, "
                f"errores={result.get('errores', 0)}."
            ),
        )
        # Guardar métricas del último ciclo para que la UI las muestre
        guardar_estado_job({
            "enabled": True, "status": "RUNNING_BY_WORKER",
            "last_run_id": result.get("manifest_path", "").rsplit("/", 1)[-1].replace("cobertura_auto_", "").replace(".jsonl", "") if result.get("manifest_path") else "",
            "last_generados": result.get("generados", 0),
            "last_actualizados": result.get("actualizados", 0),
            "last_errores": result.get("errores", 0),
            "last_manifest_path": result.get("manifest_path", ""),
            "detalle": (
                f"Pasada terminada. "
                f"Generados={result.get('generados', 0)}, "
                f"Actualizados={result.get('actualizados', 0)}, "
                f"Errores={result.get('errores', 0)}."
            ),
        })
    except ProcesoCoberturaYaEnEjecucion as exc:
        estado = leer_estado_job()
        retries = int(estado.get("retry_count", 0)) + 1
        guardar_estado_job({
            "enabled": True,
            "status": "RETRY_PENDING",
            "last_error": str(exc),
            "retry_count": retries,
            "detalle": "Lock ocupado. Reintento automático.",
        })
        return 0
    except Exception as exc:
        marcar_job_reintento(str(exc))
        return 1

    pendientes_despues = contar_pendientes(username, password, fe_pla_aniomes_desde, dig_tramite)
    log(f"[INFO] Pendientes después de ejecutar: {pendientes_despues}")

    heartbeat_job(
        pendientes_despues=pendientes_despues,
        detalle=(
            f"Revisión posterior a la generación. Pendientes restantes: {pendientes_despues}."
        ),
    )

    if pendientes_despues <= 0:
        if modo_vigilante:
            marcar_job_vigilando_sin_pendientes(
                f"Terminados pendientes actuales con FE_PLA_ANIOMES >= {fe_pla_aniomes_desde}. "
                "Sistema vigilando.",
                sync_pending=False,
            )
            log("[INFO] Sin pendientes. El loop sigue vigilando.")
    else:
        guardar_estado_job(
            {
                "enabled": True,
                "status": "RETRY_PENDING",
                "pendientes_despues": pendientes_despues,
                "last_error": "",
                "retry_count": 0,
                "sync_pending": False,
                "detalle": "Aún quedan pendientes. El loop sigue ejecutando.",
            }
        )
        log("[INFO] Aún quedan pendientes. El loop sigue ejecutando.")
    return 0


def main() -> int:
    sleep_sequence = (2, 4)
    loop_index = 0

    while True:
        loop_index += 1
        loop_sleep_seconds = sleep_sequence[(loop_index - 1) % len(sleep_sequence)]

        try:
            _run_cycle()
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            log(f"[ERROR] Ciclo inesperado del recuperador: {exc}")
            time.sleep(loop_sleep_seconds)
            continue

        log(f"[INFO] Pausa del loop: {loop_sleep_seconds}s.")
        time.sleep(loop_sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
