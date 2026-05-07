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
    heartbeat_job,
    marcar_sync_activo,
    marcar_sync_finalizado,
    marcar_job_completado,
    marcar_job_reintento,
    marcar_job_vigilando_sin_pendientes,
    obtener_tramites_sync_pendientes,
    marcar_tramite_sync_ok,
    marcar_tramite_sync_error,
)
from src.cobertura_runner import (  # noqa: E402
    ejecutar_coberturas_con_lock,
    ProcesoCoberturaYaEnEjecucion,
)
from src.oracle_jdbc import oracle_connect  # noqa: E402


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


def ejecutar_sync_repo(output_dir: str, dig_tramite: str = "") -> dict:
    script = PROJECT_ROOT / "scripts" / "sync_coberturas_repo.py"
    if not script.exists():
        msg = f"No existe script de sync: {script}"
        log(f"[WARN] {msg}")
        return {"ok": False, "already_running": False, "returncode": -1, "stdout": "", "error": msg}
    cmd = [sys.executable, str(script), "--origen-root", output_dir,
           "--repo-root", "/data_nuevo/repo_grande/data/datos",
           "--logs-dir", str(PROJECT_ROOT / "logs"),
           "--state-db", str(PROJECT_ROOT / "logs" / "cobertura_repo_sync.sqlite"),
           "--backup-root", str(PROJECT_ROOT / "logs" / "sync_replaced_cc_backups"),
           "--replace-existing-cc",
           "--apply"]
    if dig_tramite:
        cmd.extend(["--tramite", dig_tramite])
    log("[INFO] Ejecutando sync al repositorio oficial...")
    try:
        estado_sync = leer_estado_job()
        heartbeat_job(
            sync_active=True,
            sync_active_since=str(estado_sync.get("sync_active_since") or estado_sync.get("updated_at") or ""),
            sync_active_tramite=str(dig_tramite or "").strip(),
            detalle="Sincronización al repositorio en ejecución.",
        )
        completed = subprocess.run(cmd, cwd=str(PROJECT_ROOT), text=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=1800, check=False)
        stdout = completed.stdout or ""
        log(stdout)
        if completed.returncode != 0:
            log(f"[WARN] Sync terminó con código {completed.returncode}")
        heartbeat_job(
            sync_active=False,
            sync_active_since="",
            sync_active_tramite="",
            detalle="Sincronización al repositorio finalizada.",
        )
        return {"ok": completed.returncode == 0, "already_running": completed.returncode == 10,
                "returncode": completed.returncode, "stdout": stdout,
                "error": "" if completed.returncode == 0 else stdout[-2000:]}
    except subprocess.TimeoutExpired as exc:
        msg = f"Timeout ejecutando sync: {exc}"
        log(f"[ERROR] {msg}")
        heartbeat_job(
            sync_active=False,
            sync_active_since="",
            sync_active_tramite="",
            last_error=msg,
            detalle="Timeout durante la sincronización al repositorio.",
        )
        return {"ok": False, "already_running": False, "returncode": -2, "stdout": "", "error": msg}
    except Exception as exc:
        msg = f"Error ejecutando sync: {exc}"
        log(f"[ERROR] {msg}")
        heartbeat_job(
            sync_active=False,
            sync_active_since="",
            sync_active_tramite="",
            last_error=msg,
            detalle="Error durante la sincronización al repositorio.",
        )
        return {"ok": False, "already_running": False, "returncode": -3, "stdout": "", "error": msg}


def _marcar_sync_pendiente(detalle: str, error: str = "") -> None:
    guardar_estado_job({"enabled": True, "status": "WATCHING_NO_PENDING",
                        "sync_pending": True, "last_error": error, "retry_count": 0, "detalle": detalle})


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    import os
    _limpiar_logs_antiguos()

    # Procesar cola de sync pendiente SIEMPRE, independiente del estado del job
    pendientes_sync = obtener_tramites_sync_pendientes(limit=500)
    if pendientes_sync:
        log(f"[INFO] Cola de sync pendiente: {len(pendientes_sync)} trámites")
        for item in pendientes_sync:
            t = item["tramite"]
            log(f"[INFO] Sincronizando trámite {t} desde cola pendiente...")
            marcar_sync_activo(t, f"Sync de cola pendiente para trámite {t}.")
            try:
                sync_result = ejecutar_sync_repo(output_dir="/data_nuevo/coberturas", dig_tramite=t)
                if sync_result.get("ok") and sync_result.get("returncode") == 0:
                    marcar_tramite_sync_ok(t)
                    log(f"[INFO] Sync OK para {t}")
                elif sync_result.get("returncode") == 20:
                    marcar_tramite_sync_error(t, "DESTINO_NO_ENCONTRADO - esperando que otra app restaure la carpeta")
                    log(f"[WARN] Destino no encontrado para {t}. Se espera a que otra app restaure.")
                else:
                    marcar_tramite_sync_error(t, sync_result.get("error", "Error desconocido"))
                    log(f"[WARN] Sync falló para {t}")
            finally:
                marcar_sync_finalizado(
                    detalle=f"Sync de cola finalizado para trámite {t}.",
                    sync_pending=True,
                )

    if not job_debe_reanudarse():
        return 0

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
        sync_pending = bool(estado.get("sync_pending"))
        if sync_pending:
            log("[INFO] Sin pendientes Oracle, pero hay sync pendiente. Ejecutando sync...")
            sync_result = ejecutar_sync_repo(output_dir=output_dir, dig_tramite=dig_tramite)
            if not sync_result.get("ok"):
                _marcar_sync_pendiente(
                    "Sin pendientes Oracle, pero la sincronización al repositorio sigue pendiente.",
                    sync_result.get("error") or f"Returncode: {sync_result.get('returncode')}")
                return 0
            if modo_vigilante:
                marcar_job_vigilando_sin_pendientes(
                    f"No hay pendientes con FE_PLA_ANIOMES >= {fe_pla_aniomes_desde}. "
                    "Sync pendiente resuelto. Sistema vigilando.", sync_pending=False)
                log("[INFO] Sync pendiente resuelto. Modo vigilante activo.")
            else:
                marcar_job_completado("Trámite específico completado con sync resuelto.")
                log("[INFO] Trámite específico completado con sync resuelto.")
            return 0

        if modo_vigilante:
            marcar_job_vigilando_sin_pendientes(
                f"No hay pendientes con FE_PLA_ANIOMES >= {fe_pla_aniomes_desde}. Sistema vigilando.",
                sync_pending=False)
            log("[INFO] Sin pendientes. Modo vigilante activo.")
        else:
            marcar_job_completado("No queda pendiente el trámite solicitado.")
            log("[INFO] Trabajo completado para trámite específico.")
        return 0

    guardar_estado_job({"enabled": True, "status": "RUNNING_BY_WORKER",
                        "last_error": "", "retry_count": 0, "watch_empty_cycles": 0,
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
        marcar_sync_finalizado(detalle="Proceso principal en ejecución detectado; se esperará al siguiente ciclo.", sync_pending=True)
        estado = leer_estado_job()
        retries = int(estado.get("retry_count", 0)) + 1
        if retries >= 5:
            guardar_estado_job({"enabled": True, "status": "RETRY_PENDING_SLOW",
                                "last_error": str(exc), "retry_count": retries,
                                "detalle": "Lock ocupado 5 veces. Reintento lento."})
        else:
            guardar_estado_job({"enabled": True, "status": "WAITING_OTHER_PROCESS",
                                "last_error": str(exc), "retry_count": retries})
        return 0
    except Exception as exc:
        marcar_sync_finalizado(detalle=f"Error en sincronización: {exc}", sync_pending=True)
        marcar_job_reintento(str(exc))
        return 1

    sync_result = ejecutar_sync_repo(output_dir=output_dir, dig_tramite=dig_tramite)
    sync_ok = bool(sync_result.get("ok"))
    sync_error = sync_result.get("error") or f"Returncode: {sync_result.get('returncode')}"
    marcar_sync_finalizado(
        detalle="Sync del ciclo principal finalizado.",
        sync_pending=not sync_ok,
    )

    pendientes_despues = contar_pendientes(username, password, fe_pla_aniomes_desde, dig_tramite)
    log(f"[INFO] Pendientes después de ejecutar: {pendientes_despues}")

    heartbeat_job(
        pendientes_despues=pendientes_despues,
        detalle=(
            f"Revisión posterior a la generación. Pendientes restantes: {pendientes_despues}."
        ),
    )

    if pendientes_despues <= 0:
        if not sync_ok:
            _marcar_sync_pendiente(
                "Se terminaron los pendientes Oracle, pero el sync al repositorio quedó pendiente.",
                sync_error)
            log("[WARN] Sin pendientes Oracle, pero sync pendiente.")
            return 0
        if modo_vigilante:
            marcar_job_vigilando_sin_pendientes(
                f"Terminados pendientes actuales con FE_PLA_ANIOMES >= {fe_pla_aniomes_desde}. "
                "Sistema vigilando.", sync_pending=False)
            log("[INFO] Pendientes actuales terminados. Modo vigilante activo.")
        else:
            marcar_job_completado("Proceso terminado para el trámite solicitado.")
            log("[INFO] Trabajo completado para trámite específico.")
    else:
        guardar_estado_job({"enabled": True, "status": "RETRY_PENDING",
                            "pendientes_despues": pendientes_despues,
                            "last_error": "" if sync_ok else sync_error, "retry_count": 0,
                            "sync_pending": not sync_ok,
                            "detalle": "Aún quedan pendientes. El timer volverá a ejecutar."
                                       + (" Sync pendiente." if not sync_ok else "")})
        log("[INFO] Aún quedan pendientes. El timer volverá a ejecutar.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
