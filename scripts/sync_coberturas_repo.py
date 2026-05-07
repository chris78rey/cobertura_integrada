#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

PDF_CC_REGEX = re.compile(r"^CC(?:_\d{2})?\.pdf$")
PDF_CC_LEGACY_REGEX = re.compile(r"^CC(?:\d+)?\.pdf$", re.IGNORECASE)


# =========================
# NUEVO: eventos vivos para Streamlit
# =========================
EMIT_JSON_EVENTS = False
SYNC_BATCH_UI_SIZE = 100
SYNC_EVENT_SEQUENCE = 0


def emit_sync_event(event_type: str, **payload) -> None:
    """
    Emite eventos JSON por stdout para que Streamlit pueda mostrar
    el avance vivo del sync.

    El prefijo permite distinguir estos eventos de los prints normales.
    """
    if not EMIT_JSON_EVENTS:
        return

    data = {
        "event_type": event_type,
        **payload,
    }

    print(
        "SYNC_EVENT_JSON " + json.dumps(data, ensure_ascii=False),
        flush=True,
    )


# =========================
# FIN NUEVO
# =========================


class SyncYaEnEjecucion(RuntimeError):
    pass


class ArchivoLock:
    """
    Evita que dos procesos de sync corran al mismo tiempo.
    Esto protege cuando el usuario da clic varias veces o cuando luego se agregue cron.
    """

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self.file = None

    def __enter__(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.lock_path.open("w", encoding="utf-8")

        try:
            fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SyncYaEnEjecucion(
                f"Ya existe otro proceso de sincronización ejecutándose. Lock: {self.lock_path}"
            ) from exc

        self.file.write(f"pid={os.getpid()}\n")
        self.file.write(f"started_at={datetime.now().isoformat()}\n")
        self.file.flush()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.file:
            try:
                fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
            finally:
                self.file.close()


def ahora() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)

    return h.hexdigest()


def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cobertura_repo_sync (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            dry_run INTEGER NOT NULL,
            tramite TEXT NOT NULL,
            archivo TEXT,
            origen TEXT,
            destino TEXT,
            source_sha256 TEXT,
            dest_sha256_before TEXT,
            estado TEXT NOT NULL,
            detalle TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cobertura_repo_sync_run_id
        ON cobertura_repo_sync(run_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cobertura_repo_sync_tramite
        ON cobertura_repo_sync(tramite)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cobertura_repo_sync_estado
        ON cobertura_repo_sync(estado)
        """
    )
    conn.commit()
    return conn


def registrar(
    conn: sqlite3.Connection,
    writer: csv.DictWriter,
    run_id: str,
    dry_run: bool,
    tramite: str,
    archivo: str,
    origen: str,
    destino: str,
    source_sha256: str,
    dest_sha256_before: str,
    estado: str,
    detalle: str,
) -> None:
    row = {
        "run_id": run_id,
        "dry_run": 1 if dry_run else 0,
        "tramite": tramite,
        "archivo": archivo,
        "origen": origen,
        "destino": destino,
        "source_sha256": source_sha256,
        "dest_sha256_before": dest_sha256_before,
        "estado": estado,
        "detalle": detalle,
        "created_at": ahora(),
    }

    conn.execute(
        """
        INSERT INTO cobertura_repo_sync (
            run_id,
            dry_run,
            tramite,
            archivo,
            origen,
            destino,
            source_sha256,
            dest_sha256_before,
            estado,
            detalle,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["run_id"],
            row["dry_run"],
            row["tramite"],
            row["archivo"],
            row["origen"],
            row["destino"],
            row["source_sha256"],
            row["dest_sha256_before"],
            row["estado"],
            row["detalle"],
            row["created_at"],
        ),
    )
    conn.commit()

    writer.writerow(row)

    # =========================
    # NUEVO: enviar cada resultado a Streamlit
    # =========================
    global SYNC_EVENT_SEQUENCE

    if EMIT_JSON_EVENTS:
        SYNC_EVENT_SEQUENCE += 1

        emit_sync_event(
            "FILE_RESULT",
            sequence=SYNC_EVENT_SEQUENCE,
            **row,
        )

        if SYNC_EVENT_SEQUENCE % SYNC_BATCH_UI_SIZE == 0:
            emit_sync_event(
                "BATCH_MARK",
                sequence=SYNC_EVENT_SEQUENCE,
                batch_size=SYNC_BATCH_UI_SIZE,
                mensaje=f"Bloque de {SYNC_BATCH_UI_SIZE} resultados emitido.",
            )
    # =========================
    # FIN NUEVO
    # =========================


def es_pdf_cc(path: Path) -> bool:
    return path.is_file() and PDF_CC_REGEX.fullmatch(path.name) is not None


def es_pdf_cc_legacy(path: Path) -> bool:
    return path.is_file() and PDF_CC_LEGACY_REGEX.fullmatch(path.name) is not None and not es_pdf_cc(path)


def reemplazar_cc_en_destino_con_backup(
    *,
    destino_dir: Path,
    pdfs: list[Path],
    backup_root: Path,
    run_id: str,
    tramite: str,
) -> dict:
    """
    Reemplaza únicamente los PDFs CC*.pdf del destino.

    No toca otros archivos de la carpeta destino.
    Deja respaldo reversible de los CC existentes y de los nuevos PDFs copiados.
    """

    destino_dir = destino_dir.resolve()
    backup_dir = backup_root / run_id / tramite
    staging_dir = backup_dir / "nuevos_verificados"

    backup_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    existentes_cc = sorted([p for p in destino_dir.iterdir() if es_pdf_cc(p)])
    legacy_cc = sorted([p for p in destino_dir.iterdir() if es_pdf_cc_legacy(p)])
    reemplazables_cc = sorted(existentes_cc + legacy_cc, key=lambda p: p.name)

    manifest = {
        "run_id": run_id,
        "tramite": tramite,
        "destino_dir": str(destino_dir),
        "backup_dir": str(backup_dir),
        "staging_dir": str(staging_dir),
        "existentes_respaldados": [],
        "legacy_respaldados": [],
        "nuevos_verificados": [],
        "eliminados_destino": [],
        "copiados_nuevos": [],
        "restauracion_por_error": [],
        "otros_archivos_no_tocados": [],
    }

    for item in sorted(destino_dir.iterdir()):
        if item.is_file() and not es_pdf_cc(item):
            manifest["otros_archivos_no_tocados"].append(item.name)

    for old_pdf in reemplazables_cc:
        backup_pdf = backup_dir / old_pdf.name
        if backup_pdf.exists():
            backup_pdf = backup_dir / f"{old_pdf.stem}_{datetime.now().strftime('%H%M%S_%f')}{old_pdf.suffix}"
        shutil.copy2(old_pdf, backup_pdf)
        entry = {
            "archivo": old_pdf.name,
            "origen": str(old_pdf),
            "backup": str(backup_pdf),
            "sha256": sha256_file(backup_pdf),
        }
        if es_pdf_cc_legacy(old_pdf):
            manifest["legacy_respaldados"].append(entry)
        else:
            manifest["existentes_respaldados"].append(
                entry
            )

    for src_pdf in pdfs:
        staged_pdf = staging_dir / src_pdf.name
        shutil.copy2(src_pdf, staged_pdf)
        src_hash = sha256_file(src_pdf)
        staged_hash = sha256_file(staged_pdf)
        if src_hash != staged_hash:
            raise RuntimeError(f"Hash no coincide en staging para {src_pdf.name}")
        manifest["nuevos_verificados"].append(
            {
                "archivo": src_pdf.name,
                "origen": str(src_pdf),
                "staging": str(staged_pdf),
                "sha256": staged_hash,
            }
        )

    try:
        for old_pdf in reemplazables_cc:
            old_pdf.unlink()
            manifest["eliminados_destino"].append(str(old_pdf))

        for staged_pdf in sorted(staging_dir.iterdir()):
            if not es_pdf_cc(staged_pdf):
                continue
            dst_pdf = destino_dir / staged_pdf.name
            shutil.copy2(staged_pdf, dst_pdf)
            staged_hash = sha256_file(staged_pdf)
            dst_hash = sha256_file(dst_pdf)
            if staged_hash != dst_hash:
                raise RuntimeError(f"Hash no coincide luego de copiar {staged_pdf.name}")
            manifest["copiados_nuevos"].append(
                {
                    "archivo": staged_pdf.name,
                    "origen": str(staged_pdf),
                    "destino": str(dst_pdf),
                    "sha256": dst_hash,
                }
            )
    except Exception as exc:
        for current_cc in sorted([p for p in destino_dir.iterdir() if es_pdf_cc(p)]):
            try:
                current_cc.unlink()
            except Exception:
                pass

        for item in manifest["existentes_respaldados"] + manifest["legacy_respaldados"]:
            backup_pdf = Path(item["backup"])
            restore_pdf = destino_dir / backup_pdf.name
            try:
                shutil.copy2(backup_pdf, restore_pdf)
                manifest["restauracion_por_error"].append(
                    {
                        "archivo": backup_pdf.name,
                        "backup": str(backup_pdf),
                        "restaurado": str(restore_pdf),
                    }
                )
            except Exception as restore_exc:
                manifest["restauracion_por_error"].append(
                    {
                        "archivo": backup_pdf.name,
                        "backup": str(backup_pdf),
                        "error_restaurando": str(restore_exc),
                    }
                )

        manifest_path = backup_dir / "manifest_reemplazo_cc_error.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        raise RuntimeError(
            f"Falló el reemplazo de CC*.pdf. Se intentó restaurar. Detalle: {exc}"
        ) from exc

    manifest_path = backup_dir / "manifest_reemplazo_cc.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return {
        "ok": True,
        "run_id": run_id,
        "backup_dir": str(backup_dir),
        "manifest_path": str(manifest_path),
        "eliminados": len(manifest["eliminados_destino"]),
        "copiados": len(manifest["copiados_nuevos"]),
        "otros_no_tocados": len(manifest["otros_archivos_no_tocados"]),
        "detalle": manifest,
    }


def construir_indice_destinos(repo_root: Path) -> dict[str, list[Path]]:
    """
    Recorre una sola vez el repositorio oficial.
    Crea un índice:
      tramite -> [rutas destino encontradas]
    """
    indice: dict[str, list[Path]] = {}

    for path in repo_root.rglob("*"):
        if not path.is_dir():
            continue

        if not re.fullmatch(r"\d{1,30}", path.name):
            continue

        indice.setdefault(path.name, []).append(path)

    for tramite in indice:
        indice[tramite] = sorted(indice[tramite])

    return indice


def buscar_destinos_por_tramite(
    indice_destinos: dict[str, list[Path]],
    tramite: str,
) -> list[Path]:
    return indice_destinos.get(tramite, [])


def listar_tramites(origen_root: Path, tramite: str | None) -> list[str]:
    if tramite:
        if not re.fullmatch(r"\d{1,30}", tramite):
            raise ValueError("El trámite debe contener solo números.")
        return [tramite]

    tramites: list[str] = []

    for item in origen_root.iterdir():
        if item.is_dir() and re.fullmatch(r"\d{1,30}", item.name):
            tramites.append(item.name)

    return sorted(tramites)


def leer_manifest_origen(origen_dir: Path) -> dict:
    manifest_path = origen_dir / ".cobertura_cc_input_manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def inferir_repo_root_scoped(repo_root: Path, origen_dir: Path) -> Path:
    manifest = leer_manifest_origen(origen_dir)
    payload = manifest.get("payload") if isinstance(manifest, dict) else {}
    fe_pla_aniomes = str((payload or {}).get("fe_pla_aniomes", "")).strip()
    year = fe_pla_aniomes[:4]
    if re.fullmatch(r"\d{4}", year):
        candidate = repo_root / year
        if candidate.exists():
            return candidate
    return repo_root


def procesar_tramite(
    conn: sqlite3.Connection,
    writer: csv.DictWriter,
    run_id: str,
    dry_run: bool,
    origen_root: Path,
    indice_destinos: dict[str, list[Path]],
    backup_root: Path,
    replace_existing_cc: bool,
    tramite: str,
) -> dict[str, int]:
    origen_dir = origen_root / tramite

    resumen = {
        "pdfs_origen": 0,
        "copiados": 0,
        "simulados": 0,
        "omitidos_existentes": 0,
        "fallidos": 0,
    }

    if not origen_dir.exists() or not origen_dir.is_dir():
        registrar(
            conn,
            writer,
            run_id,
            dry_run,
            tramite,
            "",
            str(origen_dir),
            "",
            "",
            "",
            "ORIGEN_NO_EXISTE",
            "No existe la carpeta origen del trámite.",
        )
        resumen["fallidos"] += 1
        return resumen

    pdfs = sorted([p for p in origen_dir.iterdir() if es_pdf_cc(p)])
    resumen["pdfs_origen"] = len(pdfs)

    if not pdfs:
        registrar(
            conn,
            writer,
            run_id,
            dry_run,
            tramite,
            "",
            str(origen_dir),
            "",
            "",
            "",
            "SIN_PDFS_CC",
            "La carpeta origen existe, pero no contiene CC.pdf, CC_01.pdf, CC_02.pdf, etc.",
        )
        resumen["fallidos"] += 1
        return resumen

    destinos = buscar_destinos_por_tramite(indice_destinos, tramite)

    if not destinos:
        for pdf in pdfs:
            registrar(
                conn,
                writer,
                run_id,
                dry_run,
                tramite,
                pdf.name,
                str(pdf),
                "",
                sha256_file(pdf),
                "",
                "DESTINO_NO_ENCONTRADO",
                "No se encontró carpeta destino con el mismo nombre del trámite dentro del repositorio oficial.",
            )
        resumen["fallidos"] += len(pdfs)
        return resumen

    if len(destinos) > 1:
        destinos_texto = " | ".join(str(d) for d in destinos)

        for pdf in pdfs:
            registrar(
                conn,
                writer,
                run_id,
                dry_run,
                tramite,
                pdf.name,
                str(pdf),
                destinos_texto,
                sha256_file(pdf),
                "",
                "DESTINO_AMBIGUO",
                "Se encontraron varias carpetas destino con el mismo trámite. No se copia por seguridad.",
            )
        resumen["fallidos"] += len(pdfs)
        return resumen

    destino_dir = destinos[0]
    destination_cc = sorted([p for p in destino_dir.iterdir() if es_pdf_cc(p)])
    legacy_cc = sorted([p for p in destino_dir.iterdir() if es_pdf_cc_legacy(p)])
    source_names = {pdf.name for pdf in pdfs}
    destination_names = {pdf.name for pdf in destination_cc}
    destination_identical = bool(destination_cc) and not legacy_cc and source_names == destination_names and all(
        (destino_dir / pdf.name).exists() and sha256_file(pdf) == sha256_file(destino_dir / pdf.name)
        for pdf in pdfs
    )

    if replace_existing_cc and (destination_cc or legacy_cc) and not destination_identical:
        if dry_run:
            for old_pdf in destination_cc:
                registrar(
                    conn,
                    writer,
                    run_id,
                    dry_run,
                    tramite,
                    old_pdf.name,
                    "",
                    str(old_pdf),
                    "",
                    sha256_file(old_pdf),
                    "SIMULADO_RESPALDARIA_CC_OBSOLETO",
                    "Simulación: el CC*.pdf del destino sería respaldado y retirado antes del reemplazo.",
                )
            for pdf in pdfs:
                destino_pdf = destino_dir / pdf.name
                source_hash = sha256_file(pdf)
                dest_hash = sha256_file(destino_pdf) if destino_pdf.exists() else ""
                registrar(
                    conn,
                    writer,
                    run_id,
                    dry_run,
                    tramite,
                    pdf.name,
                    str(pdf),
                    str(destino_pdf),
                    source_hash,
                    dest_hash,
                    "SIMULADO_REEMPLAZARIA_CC",
                    "Simulación: el CC*.pdf del destino sería reemplazado con respaldo reversible.",
                )
            for old_pdf in legacy_cc:
                registrar(
                    conn,
                    writer,
                    run_id,
                    dry_run,
                    tramite,
                    old_pdf.name,
                    "",
                    str(old_pdf),
                    "",
                    sha256_file(old_pdf),
                    "SIMULADO_RESPALDARIA_CC_OBSOLETO_LEGACY",
                    "Simulación: el CC legacy del destino sería respaldado y retirado antes del reemplazo.",
                )
            resumen["simulados"] += len(pdfs) + len(destination_cc) + len(legacy_cc)
            return resumen

        try:
            reemplazo = reemplazar_cc_en_destino_con_backup(
                destino_dir=destino_dir,
                pdfs=pdfs,
                backup_root=backup_root,
                run_id=run_id,
                tramite=tramite,
            )
        except Exception as exc:
            detalle = f"Error reemplazando CC*.pdf con backup: {exc}"
            for pdf in pdfs:
                registrar(
                    conn,
                    writer,
                    run_id,
                    dry_run,
                    tramite,
                    pdf.name,
                    str(pdf),
                    str(destino_dir / pdf.name),
                    sha256_file(pdf),
                    "",
                    "ERROR_REEMPLAZO_CC",
                    detalle,
                )
            resumen["fallidos"] += len(pdfs)
            return resumen

        for item in reemplazo["detalle"]["existentes_respaldados"]:
            registrar(
                conn,
                writer,
                run_id,
                dry_run,
                tramite,
                item["archivo"],
                item["origen"],
                item["backup"],
                item["sha256"],
                "",
                "RESPALDADO_CC_OBSOLETO",
                "CC*.pdf obsoleto respaldado y retirado del destino antes del reemplazo.",
            )

        for item in reemplazo["detalle"]["copiados_nuevos"]:
            registrar(
                conn,
                writer,
                run_id,
                dry_run,
                tramite,
                item["archivo"],
                item["origen"],
                item["destino"],
                item["sha256"],
                item["sha256"],
                "REEMPLAZADO_CON_BACKUP",
                "CC*.pdf reemplazado en destino con respaldo reversible.",
            )

        resumen["copiados"] += len(pdfs)
        return resumen

    for pdf in pdfs:
        destino_pdf = destino_dir / pdf.name
        source_hash = sha256_file(pdf)

        if destino_pdf.exists():
            dest_hash = sha256_file(destino_pdf)

            if source_hash == dest_hash:
                estado = "OMITIDO_YA_EXISTE_IDENTICO"
                detalle = (
                    "El PDF ya existe en destino y es idéntico. No se sobrescribe."
                )
            else:
                estado = "OMITIDO_YA_EXISTE_DIFERENTE"
                detalle = "El PDF ya existe en destino, pero tiene hash diferente. No se sobrescribe por seguridad."

            registrar(
                conn,
                writer,
                run_id,
                dry_run,
                tramite,
                pdf.name,
                str(pdf),
                str(destino_pdf),
                source_hash,
                dest_hash,
                estado,
                detalle,
            )
            resumen["omitidos_existentes"] += 1
            continue

        if dry_run:
            registrar(
                conn,
                writer,
                run_id,
                dry_run,
                tramite,
                pdf.name,
                str(pdf),
                str(destino_pdf),
                source_hash,
                "",
                "SIMULADO_COPIARIA",
                "Simulación: el archivo se copiaría porque no existe en destino.",
            )
            resumen["simulados"] += 1
            continue

        try:
            shutil.copy2(pdf, destino_pdf)

            if not destino_pdf.exists():
                registrar(
                    conn,
                    writer,
                    run_id,
                    dry_run,
                    tramite,
                    pdf.name,
                    str(pdf),
                    str(destino_pdf),
                    source_hash,
                    "",
                    "ERROR_COPIA",
                    "Se intentó copiar, pero el archivo destino no existe después de la copia.",
                )
                resumen["fallidos"] += 1
                continue

            dest_hash_after = sha256_file(destino_pdf)

            if source_hash != dest_hash_after:
                registrar(
                    conn,
                    writer,
                    run_id,
                    dry_run,
                    tramite,
                    pdf.name,
                    str(pdf),
                    str(destino_pdf),
                    source_hash,
                    dest_hash_after,
                    "ERROR_HASH",
                    "El archivo se copió, pero el hash origen/destino no coincide.",
                )
                resumen["fallidos"] += 1
                continue

            registrar(
                conn,
                writer,
                run_id,
                dry_run,
                tramite,
                pdf.name,
                str(pdf),
                str(destino_pdf),
                source_hash,
                dest_hash_after,
                "COPIADO",
                "Archivo copiado correctamente y verificado por SHA256.",
            )
            resumen["copiados"] += 1

        except Exception as exc:
            registrar(
                conn,
                writer,
                run_id,
                dry_run,
                tramite,
                pdf.name,
                str(pdf),
                str(destino_pdf),
                source_hash,
                "",
                "ERROR_COPIA",
                f"Error copiando archivo: {exc}",
            )
            resumen["fallidos"] += 1

    return resumen


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sincroniza PDFs CC*.pdf desde /data_nuevo/coberturas hacia el repositorio oficial."
    )

    parser.add_argument(
        "--origen-root",
        default="/data_nuevo/coberturas",
        help="Carpeta donde están las coberturas generadas. Ejemplo: /data_nuevo/coberturas",
    )
    parser.add_argument(
        "--repo-root",
        default="/data_nuevo/repo_grande/data/datos",
        help="Raíz del repositorio oficial donde se buscarán las carpetas por trámite.",
    )
    parser.add_argument(
        "--tramite",
        default="",
        help="Trámite específico. Si se omite, procesa todas las carpetas hijas del origen.",
    )
    parser.add_argument(
        "--logs-dir",
        default="/data_nuevo/cobertura_integrada/logs",
    )
    parser.add_argument(
        "--state-db",
        default="/data_nuevo/cobertura_integrada/logs/cobertura_repo_sync.sqlite",
    )
    parser.add_argument(
        "--backup-root",
        default="/data_nuevo/cobertura_integrada/logs/sync_replaced_cc_backups",
        help="Carpeta raíz para respaldar CC*.pdf cuando se reemplazan en destino.",
    )
    parser.add_argument(
        "--replace-existing-cc",
        action="store_true",
        help="Permite respaldar y reemplazar solo CC*.pdf cuando el origen difiere del destino.",
    )

    # =========================
    # NUEVO: salida viva para Streamlit
    # =========================
    parser.add_argument(
        "--emit-json-events",
        action="store_true",
        help="Emite eventos JSON por stdout para mostrar avance en Streamlit.",
    )

    parser.add_argument(
        "--batch-ui-size",
        type=int,
        default=100,
        help="Cantidad de resultados que Streamlit mostrará como bloque visible.",
    )
    # =========================
    # FIN NUEVO
    # =========================

    modo = parser.add_mutually_exclusive_group(required=True)
    modo.add_argument("--dry-run", action="store_true")
    modo.add_argument("--apply", action="store_true")

    args = parser.parse_args()

    origen_root = Path(args.origen_root).resolve()
    repo_root = Path(args.repo_root).resolve()
    logs_dir = Path(args.logs_dir).resolve()
    state_db = Path(args.state_db).resolve()
    backup_root = Path(args.backup_root).resolve()
    tramite = args.tramite.strip() or None
    dry_run = bool(args.dry_run)
    replace_existing_cc = bool(args.replace_existing_cc)

    # =========================
    # NUEVO: configuración de eventos vivos
    # =========================
    global EMIT_JSON_EVENTS
    global SYNC_BATCH_UI_SIZE

    EMIT_JSON_EVENTS = bool(args.emit_json_events)
    SYNC_BATCH_UI_SIZE = max(1, int(args.batch_ui_size or 100))
    # =========================
    # FIN NUEVO
    # =========================

    if not origen_root.exists():
        raise RuntimeError(f"No existe origen-root: {origen_root}")

    if not repo_root.exists():
        raise RuntimeError(f"No existe repo-root: {repo_root}")

    logs_dir.mkdir(parents=True, exist_ok=True)
    backup_root.mkdir(parents=True, exist_ok=True)
    lock_path = logs_dir / "cobertura_repo_sync.lock"

    repo_scan_root = repo_root
    if tramite:
        repo_scan_root = inferir_repo_root_scoped(repo_root, origen_root / tramite)
        if repo_scan_root != repo_root:
            print(f"Acotando búsqueda de destinos a {repo_scan_root} según manifiesto local.")

    try:
        with ArchivoLock(lock_path):
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            emit_sync_event(
                "RUN_START",
                run_id=run_id,
                dry_run=dry_run,
                origen_root=str(origen_root),
                repo_root=str(repo_scan_root),
                tramite=tramite or "",
            )
            modo_nombre = "dry_run" if dry_run else "apply"
            manifest_path = logs_dir / f"cobertura_repo_sync_{modo_nombre}_{run_id}.csv"

            tramites = listar_tramites(origen_root, tramite)

            print(f"Trámites origen encontrados en {origen_root}: {len(tramites)}")
            print(f"Construyendo índice de carpetas destino en {repo_scan_root}...")

            indice_destinos = construir_indice_destinos(repo_scan_root)

            print(f"Carpetas destino indexadas: {len(indice_destinos)}")

            emit_sync_event(
                "INDEX_FINISHED",
                run_id=run_id,
                tramites_origen=len(tramites),
                carpetas_destino_indexadas=len(indice_destinos),
            )

            conn = init_db(state_db)

            totales = {
                "tramites": len(tramites),
                "pdfs_origen": 0,
                "copiados": 0,
                "simulados": 0,
                "omitidos_existentes": 0,
                "fallidos": 0,
            }

            with manifest_path.open("w", encoding="utf-8", newline="") as csv_file:
                writer = csv.DictWriter(
                    csv_file,
                    fieldnames=[
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
                    ],
                )
                writer.writeheader()

                for index, item_tramite in enumerate(tramites, start=1):
                    print(f"[{index}/{len(tramites)}] Revisando trámite {item_tramite}")

                    emit_sync_event(
                        "TRAMITE_START",
                        run_id=run_id,
                        index=index,
                        total_tramites=len(tramites),
                        tramite=item_tramite,
                    )

                    resumen = procesar_tramite(
                        conn=conn,
                        writer=writer,
                        run_id=run_id,
                        dry_run=dry_run,
                        origen_root=origen_root,
                        indice_destinos=indice_destinos,
                        backup_root=backup_root,
                        replace_existing_cc=replace_existing_cc,
                        tramite=item_tramite,
                    )

                    for key in resumen:
                        totales[key] += resumen[key]

            conn.close()

            print("")
            print("Proceso finalizado")
            print(f"Modo: {'SIMULACIÓN' if dry_run else 'COPIA REAL'}")
            print(f"Run ID: {run_id}")
            print(f"Trámites revisados: {totales['tramites']}")
            print(f"PDFs origen encontrados: {totales['pdfs_origen']}")
            print(f"Copiados: {totales['copiados']}")
            print(f"Simulados copiaría: {totales['simulados']}")
            print(f"Omitidos porque ya existen: {totales['omitidos_existentes']}")
            print(f"Fallidos / no pasaron: {totales['fallidos']}")
            print(f"CSV detalle: {manifest_path}")
            print(f"SQLite histórico: {state_db}")

            emit_sync_event(
                "RUN_END",
                run_id=run_id,
                manifest_path=str(manifest_path),
                state_db=str(state_db),
                **totales,
            )

    except SyncYaEnEjecucion as exc:
        print(f"SYNC_YA_EN_EJECUCION: {exc}")
        return 10

    # Si se procesó un trámite específico y hubo fallidos, retornar 20
    if tramite and totales.get("fallidos", 0) > 0:
        print("SYNC_CON_FALLIDOS_TRAMITE_ESPECIFICO")
        return 20

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
