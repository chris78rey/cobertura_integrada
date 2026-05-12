from __future__ import annotations

import csv
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
_INDICE_DESTINOS_CACHE: dict[str, dict[str, list[Path]]] = {}


def _ejecutar_sync_repo_subproceso(
    output_dir: str,
    dig_tramite: str = "",
    *,
    project_root: Path | None = None,
    repo_root: str = "/data_nuevo/repo_grande/data/datos",
    backup_root: str | None = None,
    replace_existing_cc: bool = True,
    timeout_seconds: int = 1800,
) -> dict[str, Any]:
    root = Path(project_root or PROJECT_ROOT).resolve()
    script = root / "scripts" / "sync_coberturas_repo.py"
    if not script.exists():
        msg = f"No existe script de sync: {script}"
        return {
            "ok": False,
            "already_running": False,
            "returncode": -1,
            "stdout": "",
            "error": msg,
        }

    cmd = [
        sys.executable,
        str(script),
        "--origen-root",
        output_dir,
        "--repo-root",
        repo_root,
        "--logs-dir",
        str(root / "logs"),
        "--state-db",
        str(root / "logs" / "cobertura_repo_sync.sqlite"),
    ]

    if backup_root:
        cmd.extend(["--backup-root", backup_root])
    else:
        cmd.extend(["--backup-root", str(root / "logs" / "sync_replaced_cc_backups")])

    if replace_existing_cc:
        cmd.append("--replace-existing-cc")

    cmd.append("--apply")

    if dig_tramite:
        cmd.extend(["--tramite", dig_tramite])

    try:
        completed = subprocess.run(
            cmd,
            cwd=str(root),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_seconds,
            check=False,
        )
        stdout = completed.stdout or ""
        if completed.returncode != 0 and stdout:
            error = stdout[-2000:]
        else:
            error = ""
        return {
            "ok": completed.returncode == 0,
            "already_running": completed.returncode == 10,
            "returncode": completed.returncode,
            "stdout": stdout,
            "error": error,
        }
    except subprocess.TimeoutExpired as exc:
        msg = f"Timeout ejecutando sync: {exc}"
        return {
            "ok": False,
            "already_running": False,
            "returncode": -2,
            "stdout": "",
            "error": msg,
        }
    except Exception as exc:
        msg = f"Error ejecutando sync: {exc}"
        return {
            "ok": False,
            "already_running": False,
            "returncode": -3,
            "stdout": "",
            "error": msg,
        }


def ejecutar_sync_repo_directo_rapido(
    output_dir: str,
    dig_tramite: str,
    *,
    project_root: Path | None = None,
    repo_root: str = "/data_nuevo/repo_grande/data/datos",
    backup_root: str | None = None,
    replace_existing_cc: bool = True,
) -> dict[str, Any]:
    """
    Sincronización rápida para un trámite específico.

    Reutiliza la lógica existente de scripts/sync_coberturas_repo.py dentro del
    mismo proceso para evitar el arranque de un subproceso por cada trámite y
    mantener el índice de destinos en caché mientras el worker siga vivo.
    """

    tramite = str(dig_tramite or "").strip()
    if not tramite:
        return {
            "ok": False,
            "already_running": False,
            "returncode": -4,
            "stdout": "",
            "error": "No se recibió DIG_TRAMITE para sync rápido.",
        }

    root = Path(project_root or PROJECT_ROOT).resolve()
    origen_root = Path(output_dir).resolve()
    repo_root_path = Path(repo_root).resolve()
    logs_dir = root / "logs"
    backup_root_path = Path(backup_root).resolve() if backup_root else logs_dir / "sync_replaced_cc_backups"
    state_db = logs_dir / "cobertura_repo_sync.sqlite"

    logs_dir.mkdir(parents=True, exist_ok=True)
    backup_root_path.mkdir(parents=True, exist_ok=True)

    try:
        from scripts.sync_coberturas_repo import (
            ArchivoLock,
            SyncYaEnEjecucion,
            construir_indice_destinos,
            inferir_repo_root_scoped,
            init_db,
            procesar_tramite,
        )
    except Exception as exc:
        return {
            "ok": False,
            "already_running": False,
            "returncode": -5,
            "stdout": "",
            "error": f"No se pudo importar sync rápido: {exc}",
        }

    origen_dir = origen_root / tramite
    repo_scan_root = inferir_repo_root_scoped(repo_root_path, origen_dir)

    lock_path = logs_dir / "cobertura_repo_sync.lock"
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    manifest_path = logs_dir / f"cobertura_repo_sync_fast_{run_id}_{tramite}.csv"

    fieldnames = [
        "run_id",
        "dry_run",
        "tramite",
        "archivo",
        "origen",
        "destino",
        "source_sha256",
        "dest_sha256_before",
        "estado",
        "detalle",
        "created_at",
    ]

    try:
        with ArchivoLock(lock_path):
            cache_key = str(repo_scan_root)
            if cache_key not in _INDICE_DESTINOS_CACHE:
                _INDICE_DESTINOS_CACHE[cache_key] = construir_indice_destinos(repo_scan_root)

            indice_destinos = _INDICE_DESTINOS_CACHE[cache_key]
            conn = init_db(state_db)

            try:
                with manifest_path.open("w", encoding="utf-8", newline="") as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                    writer.writeheader()

                    resumen = procesar_tramite(
                        conn=conn,
                        writer=writer,
                        run_id=run_id,
                        dry_run=False,
                        origen_root=origen_root,
                        indice_destinos=indice_destinos,
                        backup_root=backup_root_path,
                        replace_existing_cc=replace_existing_cc,
                        tramite=tramite,
                    )
            finally:
                conn.close()

        fallidos = int(resumen.get("fallidos", 0) or 0)
        stdout = (
            f"SYNC_FAST tramite={tramite} "
            f"copiados={resumen.get('copiados', 0)} "
            f"omitidos={resumen.get('omitidos_existentes', 0)} "
            f"fallidos={fallidos} "
            f"manifest={manifest_path}"
        )

        return {
            "ok": fallidos == 0,
            "already_running": False,
            "returncode": 0 if fallidos == 0 else 20,
            "stdout": stdout,
            "error": "" if fallidos == 0 else stdout,
            "fast": True,
            "manifest_path": str(manifest_path),
            "resumen": resumen,
        }

    except SyncYaEnEjecucion as exc:
        return {
            "ok": False,
            "already_running": True,
            "returncode": 10,
            "stdout": "",
            "error": str(exc),
            "fast": True,
        }
    except Exception as exc:
        return {
            "ok": False,
            "already_running": False,
            "returncode": -6,
            "stdout": "",
            "error": f"Error en sync rápido para trámite {tramite}: {exc}",
            "fast": True,
        }


def ejecutar_sync_repo(
    output_dir: str,
    dig_tramite: str = "",
    *,
    project_root: Path | None = None,
    repo_root: str = "/data_nuevo/repo_grande/data/datos",
    backup_root: str | None = None,
    replace_existing_cc: bool = True,
    timeout_seconds: int = 1800,
) -> dict[str, Any]:
    """
    Mantiene la firma original.

    Para trámite específico usa sync rápido en memoria.
    Si falla por importación o por error técnico del modo rápido,
    cae al camino clásico por subproceso.
    """

    if str(dig_tramite or "").strip():
        resultado_rapido = ejecutar_sync_repo_directo_rapido(
            output_dir=output_dir,
            dig_tramite=dig_tramite,
            project_root=project_root,
            repo_root=repo_root,
            backup_root=backup_root,
            replace_existing_cc=replace_existing_cc,
        )

        if resultado_rapido.get("returncode") not in {-5, -6}:
            return resultado_rapido

    return _ejecutar_sync_repo_subproceso(
        output_dir=output_dir,
        dig_tramite=dig_tramite,
        project_root=project_root,
        repo_root=repo_root,
        backup_root=backup_root,
        replace_existing_cc=replace_existing_cc,
        timeout_seconds=timeout_seconds,
    )
