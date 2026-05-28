from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from src.cobertura_runner import ArchivoLock, LOCK_PATH, ProcesoCoberturaYaEnEjecucion
from src.cobertura_pdf import (
    _parse_fecha_yyyy_mm_dd,
    _expandir_cedulas_para_cobertura,
    _limpiar_local_post_sincronizacion,
    _nombre_cc_por_secuencia,
    _resguardar_cc_locales,
    _archivar_carpeta_tramite_si_corresponde,
    _run_node_pdf_generator,
)
from src.oracle_jdbc import oracle_connect
from src.repo_sync import ejecutar_sync_repo


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
OUTPUT_ROOT = Path("/data_nuevo/coberturas")
REPO_ROOT = Path("/data_nuevo/repo_grande/data/datos")
LOGS_DIR = PROJECT_ROOT / "logs"
AUDIT_LOG = LOGS_DIR / "auditoria_menor_edad_sync.jsonl"
BACKUP_CC_ROOT = LOGS_DIR / "backup_auditoria_menor_edad_cc"
PDF_CC_REGEX = re.compile(r"^CC(?:_\d{2})?\.pdf$", re.IGNORECASE)
PDF_CC_LEGACY_REGEXES = [
    re.compile(r"^CC(?:\d+)?\.pdf$", re.IGNORECASE),
    re.compile(r"^C[1-6]\.pdf$", re.IGNORECASE),
]


def _ahora_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ahora_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _auditar_evento(evento: dict[str, Any]) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"ts": _ahora_iso(), **evento}
    with AUDIT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _validar_mes(valor: str) -> str:
    valor = str(valor or "").strip()
    if not re.fullmatch(r"\d{6}", valor):
        raise ValueError("FE_PLA_ANIOMES debe tener formato AAAAMM, por ejemplo 202605.")
    mes = int(valor[4:6])
    if mes < 1 or mes > 12:
        raise ValueError("FE_PLA_ANIOMES tiene un mes inválido.")
    return valor


def _validar_tramite(valor: str) -> str:
    valor = str(valor or "").strip()
    if not valor or not valor.isdigit():
        raise ValueError("El trámite debe contener solo números.")
    if len(valor) > 30:
        raise ValueError("El trámite no debe superar 30 dígitos.")
    return valor


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _is_pdf_cc(path: Path) -> bool:
    return path.is_file() and PDF_CC_REGEX.fullmatch(path.name) is not None


def _is_pdf_cc_legacy(path: Path) -> bool:
    return path.is_file() and any(regex.fullmatch(path.name) for regex in PDF_CC_LEGACY_REGEXES) and not _is_pdf_cc(path)


def _listar_cc(path: Path) -> list[str]:
    if not path.exists() or not path.is_dir():
        return []
    return sorted(p.name for p in path.iterdir() if _is_pdf_cc(p))


def _listar_cc_legacy(path: Path) -> list[str]:
    if not path.exists() or not path.is_dir():
        return []
    return sorted(p.name for p in path.iterdir() if _is_pdf_cc_legacy(p))


def _consultar_menores_por_mes(
    username: str,
    password: str,
    fe_pla_aniomes: str,
    max_rows: int,
) -> list[dict[str, str]]:
    conn = ps = rs = None
    sql = """
        SELECT *
        FROM (
            SELECT
                TO_CHAR(DIG_TRAMITE) AS DIG_TRAMITE,
                TRIM(NVL(DIG_CEDULA, '')) AS DIG_CEDULA,
                TRIM(NVL(DIG_DEPENDIENTE_01, '')) AS DIG_DEPENDIENTE_01,
                TRIM(NVL(DIG_DEPENDIENTE_02, '')) AS DIG_DEPENDIENTE_02,
                TRIM(TO_CHAR(DIG_FECHA_HASTA, 'YYYY-MM-DD')) AS DIG_FECHA_HASTA,
                TRIM(TO_CHAR(DIG_FECHA_ALTA, 'YYYY-MM-DD')) AS DIG_FECHA_ALTA,
                TRIM(TO_CHAR(DIG_FECHA_PLANILLA, 'YYYY-MM-DD')) AS DIG_FECHA_PLANILLA,
                TRIM(NVL(DIG_MENOR_EDAD, 'N')) AS DIG_MENOR_EDAD,
                TRIM(NVL(DIG_COBERTURA, 'N')) AS DIG_COBERTURA,
                TRIM(NVL(DIG_PLANILLADO, '')) AS DIG_PLANILLADO,
                TRIM(TO_CHAR(FE_PLA_ANIOMES)) AS FE_PLA_ANIOMES
            FROM DIGITALIZACION.DIGITALIZACION
            WHERE TRIM(TO_CHAR(FE_PLA_ANIOMES)) = ?
              AND TRIM(NVL(DIG_MENOR_EDAD, 'N')) = 'S'
              AND TRIM(NVL(DIG_PLANILLADO, 'N')) = 'S'
            ORDER BY DIG_TRAMITE
        )
        WHERE ROWNUM <= ?
    """
    rows: list[dict[str, str]] = []
    try:
        conn = oracle_connect(username, password)
        ps = conn.jconn.prepareStatement(sql)
        ps.setString(1, fe_pla_aniomes)
        ps.setInt(2, int(max_rows))
        ps.setQueryTimeout(90)
        rs = ps.executeQuery()
        while rs.next():
            rows.append({
                "DIG_TRAMITE": str(rs.getString(1) or "").strip(),
                "DIG_CEDULA": str(rs.getString(2) or "").strip(),
                "DIG_DEPENDIENTE_01": str(rs.getString(3) or "").strip(),
                "DIG_DEPENDIENTE_02": str(rs.getString(4) or "").strip(),
                "DIG_FECHA_HASTA": str(rs.getString(5) or "").strip(),
                "DIG_FECHA_ALTA": str(rs.getString(6) or "").strip(),
                "DIG_FECHA_PLANILLA": str(rs.getString(7) or "").strip(),
                "DIG_MENOR_EDAD": str(rs.getString(8) or "").strip(),
                "DIG_COBERTURA": str(rs.getString(9) or "").strip(),
                "DIG_PLANILLADO": str(rs.getString(10) or "").strip(),
                "FE_PLA_ANIOMES": str(rs.getString(11) or "").strip(),
            })
        return rows
    finally:
        for obj in (rs, ps, conn):
            if obj:
                try:
                    obj.close()
                except Exception:
                    pass


@st.cache_data(ttl=300, show_spinner=False)
def _construir_indice_destinos_por_year(repo_root_text: str, year: str) -> dict[str, list[str]]:
    repo_root = Path(repo_root_text).resolve()
    scope = repo_root / year if year and (repo_root / year).exists() else repo_root
    indice: dict[str, list[str]] = {}
    if not scope.exists():
        return indice

    for path in scope.rglob("*"):
        if not path.is_dir():
            continue
        if not re.fullmatch(r"\d{1,30}", path.name):
            continue
        resolved = str(path.resolve())
        if resolved.startswith(str(scope.resolve())):
            indice.setdefault(path.name, []).append(resolved)

    return {k: sorted(v) for k, v in indice.items()}


def _preparar_registro_cobertura(row: dict[str, str]) -> dict[str, str]:
    return {
        "dig_tramite": str(row.get("DIG_TRAMITE", "")).strip(),
        "dig_cedula": str(row.get("DIG_CEDULA", "")).strip(),
        "dig_menor_edad": str(row.get("DIG_MENOR_EDAD", "")).strip(),
        "dig_dependiente_01": str(row.get("DIG_DEPENDIENTE_01", "")).strip(),
        "dig_dependiente_02": str(row.get("DIG_DEPENDIENTE_02", "")).strip(),
        "dig_fecha_hasta": str(row.get("DIG_FECHA_HASTA", "")).strip(),
        "dig_fecha_alta": str(row.get("DIG_FECHA_ALTA", "")).strip(),
        "dig_fecha_planilla": str(row.get("DIG_FECHA_PLANILLA", "")).strip(),
    }


def _esperados_cobertura(row: dict[str, str]) -> tuple[dict[str, str], list[dict[str, str]], list[str]]:
    registro = _preparar_registro_cobertura(row)
    cedulas_a_generar = _expandir_cedulas_para_cobertura(registro)
    expected_output_names = [
        _nombre_cc_por_secuencia(indice=i, total=len(cedulas_a_generar))
        for i in range(1, len(cedulas_a_generar) + 1)
    ]
    return registro, cedulas_a_generar, [f"{name}.pdf" for name in expected_output_names]


def _auditar_fila(row: dict[str, str], indice_destinos: dict[str, list[str]]) -> dict[str, Any]:
    tramite = str(row.get("DIG_TRAMITE", "")).strip()
    local_dir = OUTPUT_ROOT / tramite
    destinos = [Path(p) for p in indice_destinos.get(tramite, [])]
    destino_dir = destinos[0] if len(destinos) == 1 else None
    registro, cedulas_a_generar, esperado = _esperados_cobertura(row)

    local_cc = _listar_cc(local_dir)
    local_legacy = _listar_cc_legacy(local_dir)
    local_todos = sorted({*local_cc, *local_legacy})
    destino_cc = _listar_cc(destino_dir) if destino_dir else []
    legacy_destino = _listar_cc_legacy(destino_dir) if destino_dir else []

    local_canonico_completo = all(x in local_cc for x in esperado)
    local_tiene_suficientes = len(local_todos) >= len(esperado)
    faltan_local = [] if local_tiene_suficientes else esperado
    faltan_destino = [x for x in esperado if x not in destino_cc]
    extras_local = [x for x in local_cc if x not in esperado]
    extras_destino = [x for x in destino_cc if x not in esperado]

    cedulas = [
        registro["dig_cedula"],
        registro["dig_dependiente_01"],
        registro["dig_dependiente_02"],
    ]
    oracle_completo = all(cedulas)

    if not oracle_completo:
        estado = "ORACLE_INCOMPLETO"
        accion = "Corregir cédulas en Oracle."
    elif not local_dir.exists():
        estado = "SIN_CARPETA_LOCAL"
        accion = "Regenerar los PDFs locales esperados y sincronizar."
    elif not local_tiene_suficientes:
        estado = "FALTAN_CC_LOCALES"
        accion = "Regenerar los PDFs locales esperados y sincronizar."
    elif local_legacy:
        estado = "LOCAL_LEGACY_CC"
        if local_canonico_completo:
            accion = "Los PDFs locales existen, pero también hay nombres legacy. Se puede sincronizar o normalizar."
        else:
            accion = "Los PDFs locales existen, pero están en formato legacy. Regenerar los PDFs locales esperados y sincronizar."
    elif len(destinos) == 0:
        estado = "DESTINO_NO_EXISTE"
        accion = "La carpeta destino todavía no existe."
    elif len(destinos) > 1:
        estado = "DESTINO_AMBIGUO"
        accion = "Revisar carpetas destino duplicadas."
    elif legacy_destino:
        estado = "DESTINO_LEGACY_CC"
        accion = "Sincronizar para limpiar CC legacy y dejar solo los PDFs canónicos esperados."
    elif not faltan_destino and not extras_destino:
        estado = "OK_DESTINO"
        accion = "Sin acción."
    else:
        estado = "REQUIERE_SINCRONIZAR"
        accion = "Sincronizar solo CC*.pdf."

    return {
        **row,
        "ESTADO_AUDITORIA": estado,
        "ACCION_RECOMENDADA": accion,
        "ESPERADOS": ", ".join(esperado),
        "CC_LOCAL": ", ".join(local_todos),
        "CC_LEGACY_LOCAL": ", ".join(local_legacy),
        "CC_DESTINO": ", ".join(destino_cc),
        "CC_LEGACY_DESTINO": ", ".join(legacy_destino),
        "FALTAN_LOCAL": ", ".join(faltan_local),
        "FALTAN_DESTINO": ", ".join(faltan_destino),
        "EXTRAS_LOCAL": ", ".join(local_legacy or extras_local),
        "EXTRAS_DESTINO": ", ".join(extras_destino),
        "DESTINO_DIR": str(destino_dir) if destino_dir else "",
        "PUEDE_SINCRONIZAR": local_canonico_completo or estado in {"REQUIERE_SINCRONIZAR", "DESTINO_LEGACY_CC"},
        "PUEDE_REGENERAR": estado in {"SIN_CARPETA_LOCAL", "FALTAN_CC_LOCALES", "LOCAL_LEGACY_CC"},
        "DIG_FECHA_ALTA": registro["dig_fecha_alta"],
    }


def _backup_y_reemplazar_solo_cc(destino_dir: Path, tramite: str, nuevos_pdfs: list[Path], usuario: str) -> dict[str, Any]:
    destino_dir = destino_dir.resolve()
    repo_root = REPO_ROOT.resolve()
    if not str(destino_dir).startswith(str(repo_root)):
        raise RuntimeError(f"Destino fuera del repositorio oficial: {destino_dir}")
    if not destino_dir.exists() or not destino_dir.is_dir():
        raise RuntimeError(f"No existe carpeta destino: {destino_dir}")
    if not nuevos_pdfs:
        raise RuntimeError("No hay PDFs CC*.pdf nuevos")

    for src_pdf in nuevos_pdfs:
        if not _is_pdf_cc(src_pdf):
            raise RuntimeError(f"Archivo no permitido: {src_pdf.name}")
        if src_pdf.stat().st_size <= 0:
            raise RuntimeError(f"PDF local vacío: {src_pdf}")

    run_id = _ahora_id()
    backup_dir = BACKUP_CC_ROOT / tramite / run_id
    staging_dir = backup_dir / "nuevos_verificados"
    backup_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)
    existentes_cc = sorted([p for p in destino_dir.iterdir() if _is_pdf_cc(p)])
    legacy_cc = sorted([p for p in destino_dir.iterdir() if _is_pdf_cc_legacy(p)])
    reemplazables = sorted(existentes_cc + legacy_cc, key=lambda p: p.name)

    manifest: dict[str, Any] = {
        "run_id": run_id,
        "usuario": usuario,
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
        if item.is_file() and not _is_pdf_cc(item):
            manifest["otros_archivos_no_tocados"].append(item.name)

    for old_pdf in reemplazables:
        backup_pdf = backup_dir / old_pdf.name
        shutil.copy2(old_pdf, backup_pdf)
        entry = {
            "archivo": old_pdf.name,
            "origen": str(old_pdf),
            "backup": str(backup_pdf),
            "sha256": _sha256_file(backup_pdf),
        }
        if _is_pdf_cc_legacy(old_pdf):
            manifest["legacy_respaldados"].append(entry)
        else:
            manifest["existentes_respaldados"].append(entry)

    for src_pdf in nuevos_pdfs:
        staged_pdf = staging_dir / src_pdf.name
        shutil.copy2(src_pdf, staged_pdf)
        src_hash = _sha256_file(src_pdf)
        staged_hash = _sha256_file(staged_pdf)
        if src_hash != staged_hash:
            raise RuntimeError(f"Hash no coincide en staging para {src_pdf.name}")
        manifest["nuevos_verificados"].append({
            "archivo": src_pdf.name,
            "origen": str(src_pdf),
            "staging": str(staged_pdf),
            "sha256": staged_hash,
        })

    try:
        for old_pdf in reemplazables:
            old_pdf.unlink()
            manifest["eliminados_destino"].append(str(old_pdf))
        for staged_pdf in sorted(staging_dir.iterdir()):
            if not _is_pdf_cc(staged_pdf):
                continue
            dst_pdf = destino_dir / staged_pdf.name
            shutil.copy2(staged_pdf, dst_pdf)
            staged_hash = _sha256_file(staged_pdf)
            dst_hash = _sha256_file(dst_pdf)
            if staged_hash != dst_hash:
                raise RuntimeError(f"Hash no coincide luego de copiar {staged_pdf.name}")
            manifest["copiados_nuevos"].append({
                "archivo": staged_pdf.name,
                "origen": str(staged_pdf),
                "destino": str(dst_pdf),
                "sha256": dst_hash,
            })
    except Exception as exc:
        for current_cc in sorted([p for p in destino_dir.iterdir() if _is_pdf_cc(p)]):
            try:
                current_cc.unlink()
            except Exception:
                pass
        for item in manifest["existentes_respaldados"] + manifest["legacy_respaldados"]:
            backup_pdf = Path(item["backup"])
            restore_pdf = destino_dir / backup_pdf.name
            try:
                shutil.copy2(backup_pdf, restore_pdf)
                manifest["restauracion_por_error"].append({
                    "archivo": backup_pdf.name,
                    "restaurado": str(restore_pdf),
                })
            except Exception as restore_exc:
                manifest["restauracion_por_error"].append({
                    "archivo": backup_pdf.name,
                    "error_restaurando": str(restore_exc),
                })
        manifest_path_error = backup_dir / "manifest_auditoria_menor_edad_error.json"
        manifest_path_error.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError(f"Falló la sincronización de CC*.pdf. Se intentó restaurar. Detalle: {exc}") from exc

    manifest_path = backup_dir / "manifest_auditoria_menor_edad.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    _auditar_evento({
        "evento": "SYNC_MENOR_EDAD_CC_OK",
        "usuario": usuario,
        "tramite": tramite,
        "manifest": str(manifest_path),
    })

    return {
        "ok": True,
        "run_id": run_id,
        "backup_dir": str(backup_dir),
        "manifest_path": str(manifest_path),
        "eliminados": len(manifest["eliminados_destino"]),
        "copiados": len(manifest["copiados_nuevos"]),
        "otros_no_tocados": len(manifest["otros_archivos_no_tocados"]),
    }


def _buscar_destinos_tramite(tramite: str) -> list[Path]:
    if not REPO_ROOT.exists():
        raise RuntimeError(f"No existe REPO_ROOT: {REPO_ROOT}")

    repo_root = REPO_ROOT.resolve()
    destinos: list[Path] = []

    for p in REPO_ROOT.rglob(tramite):
        if not p.is_dir():
            continue
        if p.name != tramite:
            continue
        resolved = p.resolve()
        if str(resolved).startswith(str(repo_root)):
            destinos.append(resolved)

    return sorted(destinos)


def _regenerar_local_y_reemplazar_cc(row: dict[str, Any], usuario: str, password: str) -> dict[str, Any]:
    tramite = str(row.get("DIG_TRAMITE", "")).strip()
    if not tramite:
        raise RuntimeError("Sin DIG_TRAMITE.")

    dig_menor_edad = str(row.get("DIG_MENOR_EDAD", "")).strip()
    dig_planillado = str(row.get("DIG_PLANILLADO", "")).strip()
    dig_cedula = str(row.get("DIG_CEDULA", "")).strip()
    dig_dep1 = str(row.get("DIG_DEPENDIENTE_01", "")).strip()
    dig_dep2 = str(row.get("DIG_DEPENDIENTE_02", "")).strip()
    dig_fecha_hasta = str(row.get("DIG_FECHA_HASTA", "")).strip()
    dig_fecha_alta = str(row.get("DIG_FECHA_ALTA", "")).strip()
    dig_fecha_planilla = str(row.get("DIG_FECHA_PLANILLA", "")).strip()
    fe_pla = str(row.get("FE_PLA_ANIOMES", "")).strip()

    if dig_planillado != "S":
        raise RuntimeError(f"El trámite {tramite} no está planillado.")
    if not dig_cedula:
        raise RuntimeError(f"El trámite {tramite} no tiene cédula titular.")
    if not dig_fecha_hasta:
        raise RuntimeError(f"El trámite {tramite} no tiene DIG_FECHA_HASTA.")

    registro = {
        "dig_tramite": tramite,
        "dig_cedula": dig_cedula,
        "dig_menor_edad": dig_menor_edad,
        "dig_dependiente_01": dig_dep1,
        "dig_dependiente_02": dig_dep2,
        "fe_pla_aniomes": fe_pla,
        "dig_fecha_hasta": dig_fecha_hasta,
        "dig_fecha_alta": dig_fecha_alta,
        "dig_fecha_planilla": dig_fecha_planilla,
    }
    cedulas_a_generar = _expandir_cedulas_para_cobertura(registro)
    if not cedulas_a_generar:
        raise RuntimeError(f"No hay cédulas válidas para regenerar el trámite {tramite}.")

    fecha_planilla_dt = _parse_fecha_yyyy_mm_dd(dig_fecha_planilla)
    fecha_alta_dt = _parse_fecha_yyyy_mm_dd(dig_fecha_alta)
    if fecha_planilla_dt and fecha_alta_dt and fecha_planilla_dt < fecha_alta_dt:
        for idx, item in enumerate(cedulas_a_generar):
            if idx < len(cedulas_a_generar) // 2:
                item["fecha_pdf"] = dig_fecha_planilla
                item["fecha_tipo"] = "PLANILLA"
            else:
                item["fecha_pdf"] = dig_fecha_alta
                item["fecha_tipo"] = "ALTA"

    output_dir = OUTPUT_ROOT / tramite
    expected_output_names = [
        _nombre_cc_por_secuencia(indice=i, total=len(cedulas_a_generar))
        for i in range(1, len(cedulas_a_generar) + 1)
    ]

    run_id = _ahora_id()
    local_resguardos: list[tuple[Path, Path]] = []
    carpeta_archivada = _archivar_carpeta_tramite_si_corresponde(output_dir)
    if carpeta_archivada is not None:
        _auditar_evento(
            {
                "evento": "TRAMITE_FOLDER_ARCHIVED",
                "usuario": usuario,
                "tramite": tramite,
                "archived_path": str(carpeta_archivada),
                "active_path": str(output_dir),
            }
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    if _listar_cc(output_dir) or _listar_cc_legacy(output_dir):
        local_resguardos = _resguardar_cc_locales(
            planilla_dir=output_dir,
            run_id=run_id,
            tramite=tramite,
        )

    nuevos_pdfs: list[Path] = []
    try:
        raw_node_dir = str(os.environ.get("COBERTURA_NODE_PROJECT_DIR", "") or "").strip()
        if not raw_node_dir:
            raise RuntimeError("Falta configurar COBERTURA_NODE_PROJECT_DIR.")
        node_project_dir = Path(raw_node_dir).expanduser().resolve()
        if not node_project_dir.exists():
            raise RuntimeError(f"No existe COBERTURA_NODE_PROJECT_DIR: {node_project_dir}")

        for secuencia_pdf, item_cedula in enumerate(cedulas_a_generar, start=1):
            c = str(item_cedula.get("cedula", "")).strip()
            output_name = expected_output_names[secuencia_pdf - 1]
            pdf_path = output_dir / f"{output_name}.pdf"
            fecha_pdf = str(item_cedula.get("fecha_pdf", dig_fecha_hasta)).strip() or dig_fecha_hasta

            if pdf_path.exists() and pdf_path.stat().st_size > 0:
                nuevos_pdfs.append(pdf_path)
                continue

            if not output_dir.exists():
                output_dir.mkdir(parents=True, exist_ok=True)

            fast_mode = str(os.environ.get("COBERTURA_FAST_MODE", "1") or "").strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
                "si",
                "sí",
                "on",
            }
            timeout_node = int(os.environ.get("COBERTURA_NODE_TIMEOUT_SECONDS", "45" if fast_mode else "120") or ("45" if fast_mode else "120"))
            retries_node = int(os.environ.get("COBERTURA_NODE_MAX_RETRIES", "1" if fast_mode else "2") or ("1" if fast_mode else "2"))
            retries_node = max(3, retries_node)
            delay_node = float(os.environ.get("COBERTURA_NODE_RETRY_DELAY", "0.5" if fast_mode else "1.0") or ("0.5" if fast_mode else "1.0"))

            result_node = _run_node_pdf_generator(
                node_project_dir=node_project_dir,
                cedula=c,
                fecha_pdf=fecha_pdf,
                output_dir=output_dir,
                output_name=output_name,
                single_timeout_seconds=timeout_node,
                max_retries=retries_node,
                delay_seconds=delay_node,
                expected_pdf_path=pdf_path,
            )

            if not result_node.get("ok"):
                raise RuntimeError(
                    f"No se pudo regenerar {output_name}.pdf para el trámite {tramite}: "
                    f"{result_node.get('error') or result_node.get('stderr') or result_node.get('stdout')}"
                )

            if not pdf_path.exists() or pdf_path.stat().st_size <= 0:
                raise RuntimeError(f"El generador no dejó el PDF esperado: {pdf_path}")

            nuevos_pdfs.append(pdf_path)

        if output_dir.exists() and not any(output_dir.iterdir()):
            try:
                output_dir.rmdir()
            except Exception:
                pass

        destinos = [Path(p) for p in _buscar_destinos_tramite(tramite)]
        if len(destinos) != 1:
            raise RuntimeError(
                "No se pudo resolver un único destino para sincronizar. "
                f"Carpetas destino encontradas: {len(destinos)}"
            )

        reemplazo = _backup_y_reemplazar_solo_cc(
            destino_dir=destinos[0],
            tramite=tramite,
            nuevos_pdfs=nuevos_pdfs,
            usuario=usuario,
        )

        _auditar_evento(
            {
                "evento": "RECUPERACION_LOCAL_MENOR_EDAD_OK",
                "usuario": usuario,
                "tramite": tramite,
                "fe_pla_aniomes": fe_pla,
                "pdfs_locales": [str(p) for p in nuevos_pdfs],
                "destino": str(destinos[0]),
                "reemplazo": reemplazo.get("manifest_path", ""),
            }
        )

        return {
            "ok": True,
            "tramite": tramite,
            "pdfs_locales_generados": [str(p) for p in nuevos_pdfs],
            "destino": str(destinos[0]),
            "reemplazo": reemplazo,
            "local_resguardos": [{"backup": str(b), "original": str(o)} for b, o in local_resguardos],
        }
    except Exception:
        for pdf in nuevos_pdfs:
            try:
                if pdf.exists():
                    pdf.unlink()
            except Exception:
                pass
        for backup_path, original_path in reversed(local_resguardos):
            try:
                if backup_path.exists():
                    shutil.move(str(backup_path), str(original_path))
            except Exception:
                pass
        raise


def _sincronizar_item(row: dict[str, Any], username: str) -> dict[str, Any]:
    tramite = str(row.get("DIG_TRAMITE", "")).strip()
    destino_dir = Path(str(row.get("DESTINO_DIR", "")).strip())
    local_dir = OUTPUT_ROOT / tramite
    _, _, expected_pdf_names = _esperados_cobertura(
        {
            "DIG_TRAMITE": tramite,
            "DIG_CEDULA": str(row.get("DIG_CEDULA", "")).strip(),
            "DIG_DEPENDIENTE_01": str(row.get("DIG_DEPENDIENTE_01", "")).strip(),
            "DIG_DEPENDIENTE_02": str(row.get("DIG_DEPENDIENTE_02", "")).strip(),
            "DIG_FECHA_HASTA": str(row.get("DIG_FECHA_HASTA", "")).strip(),
            "DIG_FECHA_ALTA": str(row.get("DIG_FECHA_ALTA", "")).strip(),
            "DIG_MENOR_EDAD": str(row.get("DIG_MENOR_EDAD", "")).strip(),
        }
    )
    nuevos_pdfs = [local_dir / name for name in expected_pdf_names]
    if not all(p.exists() and p.stat().st_size > 0 for p in nuevos_pdfs):
        raise RuntimeError(f"No están completos los PDFs locales esperados en {local_dir}")
    return _backup_y_reemplazar_solo_cc(
        destino_dir=destino_dir,
        tramite=tramite,
        nuevos_pdfs=nuevos_pdfs,
        usuario=username,
    )


def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, quoting=csv.QUOTE_MINIMAL)
    return buffer.getvalue().encode("utf-8-sig")


def auditoria_menor_edad_page() -> None:
    st.markdown(
        """
        <div class="main-title">Auditoría de menores de edad</div>
        <div class="main-subtitle">Filtro obligatorio por FE_PLA_ANIOMES y sincronización segura de solo CC*.pdf.</div>
        """,
        unsafe_allow_html=True,
    )

    st.warning(
        "Esta pantalla no modifica Oracle ni regenera PDFs. Solo sincroniza CC*.pdf cuando el trámite ya tiene los archivos locales esperados.",
        icon="⚠️",
    )

    username = st.session_state.get("oracle_user", "")
    password = st.session_state.get("oracle_password", "")
    if not username or not password:
        st.error("No hay sesión de Oracle activa.")
        return

    st.markdown('<div class="simple-card">', unsafe_allow_html=True)
    col_mes, col_limite = st.columns([2, 1])
    with col_mes:
        fe_pla_aniomes = st.text_input(
            "FE_PLA_ANIOMES",
            value=st.session_state.get("audit_menor_mes", "202605"),
            max_chars=6,
        )
    with col_limite:
        max_rows = st.number_input(
            "Máximo registros",
            min_value=10,
            max_value=5000,
            value=1000,
            step=100,
        )

    solo_problemas = st.checkbox("Mostrar solo trámites con problema", value=True)

    col_buscar, col_limpiar = st.columns([3, 1])
    with col_buscar:
        buscar = st.button("Buscar menores del mes", use_container_width=True, key="btn_audit_menor_buscar")
    with col_limpiar:
        if st.button("Limpiar", use_container_width=True, key="btn_audit_menor_limpiar"):
            for key in ["audit_menor_rows", "audit_menor_mes", "audit_menor_selected"]:
                st.session_state.pop(key, None)
            st.rerun()

    if buscar:
        try:
            mes = _validar_mes(fe_pla_aniomes)
            st.session_state["audit_menor_mes"] = mes
            with st.spinner("Consultando Oracle y revisando destino..."):
                oracle_rows = _consultar_menores_por_mes(username, password, mes, int(max_rows))
                indice_destinos = _construir_indice_destinos_por_year(str(REPO_ROOT), mes[:4])
                auditados = [_auditar_fila(row, indice_destinos) for row in oracle_rows]
            st.session_state["audit_menor_rows"] = auditados
        except Exception as exc:
            st.error(str(exc))
            st.markdown("</div>", unsafe_allow_html=True)
            return

    rows = st.session_state.get("audit_menor_rows", [])
    if not rows:
        st.info("Ingresar FE_PLA_ANIOMES, por ejemplo 202605, y presionar Buscar menores del mes.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    df = pd.DataFrame(rows)
    df_view = df.copy()
    if solo_problemas:
        df_view = df_view[df_view["ESTADO_AUDITORIA"] != "OK_DESTINO"]

    total = len(df)
    problemas = int((df["ESTADO_AUDITORIA"] != "OK_DESTINO").sum())
    sincronizables = int(df["PUEDE_SINCRONIZAR"].sum())
    ok = total - problemas

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Menores revisados", total)
    c2.metric("OK destino", ok)
    c3.metric("Con problema", problemas)
    c4.metric("Sincronizables", sincronizables)

    columnas = [
        "DIG_TRAMITE",
        "FE_PLA_ANIOMES",
        "DIG_CEDULA",
        "DIG_DEPENDIENTE_01",
        "DIG_DEPENDIENTE_02",
        "DIG_FECHA_ALTA",
        "DIG_COBERTURA",
        "ESTADO_AUDITORIA",
        "CC_LEGACY_LOCAL",
        "CC_LEGACY_DESTINO",
        "FALTAN_DESTINO",
        "EXTRAS_DESTINO",
        "FALTAN_LOCAL",
        "DESTINO_DIR",
        "ACCION_RECOMENDADA",
    ]

    st.dataframe(df_view[columnas], use_container_width=True, hide_index=True)

    st.download_button(
        "Descargar reporte CSV",
        data=_df_to_csv_bytes(df_view[columnas]),
        file_name=f"auditoria_menor_edad_{st.session_state.get('audit_menor_mes', '')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    candidatos_sync = [r for r in rows if r.get("PUEDE_SINCRONIZAR")]
    candidatos_regen = [r for r in rows if r.get("PUEDE_REGENERAR")]

    if not candidatos_sync and not candidatos_regen:
        st.info("No hay trámites accionables. Los restantes requieren intervención Oracle o revisión manual.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    if candidatos_regen:
        opciones_regen = {
            f"{r['DIG_TRAMITE']} | estado: {r.get('ESTADO_AUDITORIA','-')} | destino: {r.get('DESTINO_DIR','') or '-'}": r
            for r in candidatos_regen
        }

        st.markdown("---")
        st.markdown("### Recuperación local")
        st.caption("Regenera los PDFs esperados desde Oracle y luego reemplaza solo CC*.pdf en el destino.")

        seleccion_regen = st.multiselect(
            "Seleccionar trámites a regenerar",
            options=list(opciones_regen.keys()),
            key="audit_menor_selected_recovery",
        )

        with st.form("form_auditoria_menor_regen"):
            confirmar_regen = st.checkbox("Confirmo regenerar localmente y sincronizar CC*.pdf")
            frase_regen = st.text_input("Escribir REGENERAR para confirmar", value="")
            submitted_regen = st.form_submit_button("Regenerar PDFs locales y sincronizar", use_container_width=True)

            if submitted_regen:
                if not seleccion_regen:
                    st.warning("Debe seleccionar al menos un trámite.")
                    st.markdown("</div>", unsafe_allow_html=True)
                    return
                if not confirmar_regen or frase_regen.strip().upper() != "REGENERAR":
                    st.warning("Falta confirmar y escribir REGENERAR.")
                    st.markdown("</div>", unsafe_allow_html=True)
                    return

                resultados = []
                try:
                    with ArchivoLock(LOCK_PATH):
                        for etiqueta in seleccion_regen:
                            row = opciones_regen[etiqueta]
                            try:
                                res = _regenerar_local_y_reemplazar_cc(row, username, password)
                                resultados.append({"DIG_TRAMITE": row["DIG_TRAMITE"], "ok": True, **res})
                            except ProcesoCoberturaYaEnEjecucion as exc:
                                resultados.append({"DIG_TRAMITE": row.get("DIG_TRAMITE"), "ok": False, "error": str(exc)})
                            except Exception as exc:
                                _auditar_evento({
                                    "evento": "RECUPERACION_LOCAL_MENOR_EDAD_ERROR",
                                    "usuario": username,
                                    "tramite": row.get("DIG_TRAMITE"),
                                    "error": str(exc),
                                })
                                resultados.append({
                                    "DIG_TRAMITE": row.get("DIG_TRAMITE"),
                                    "ok": False,
                                    "error": str(exc),
                                })
                except Exception as exc:
                    st.error(str(exc))
                    st.markdown("</div>", unsafe_allow_html=True)
                    return

                st.success("Recuperación local finalizada.")
                st.dataframe(pd.DataFrame(resultados), use_container_width=True, hide_index=True)
                _construir_indice_destinos_por_year.clear()
                st.session_state.pop("audit_menor_rows", None)
                st.info("Presionar Buscar menores del mes para refrescar el reporte.")

    if candidatos_sync:
        opciones = {
            f"{r['DIG_TRAMITE']} | faltan: {r.get('FALTAN_DESTINO','') or '-'} | extras: {r.get('EXTRAS_DESTINO','') or '-'}": r
            for r in candidatos_sync
        }

        st.markdown("---")
        st.markdown("### Sincronización segura")

        seleccion = st.multiselect(
            "Seleccionar trámites a sincronizar",
            options=list(opciones.keys()),
            key="audit_menor_selected",
        )

        with st.form("form_auditoria_menor_sync"):
            confirmar = st.checkbox("Confirmo sincronizar únicamente CC*.pdf de los trámites seleccionados")
            frase = st.text_input("Escribir SINCRONIZAR para confirmar", value="")
            submitted = st.form_submit_button("Sincronizar CC*.pdf seleccionados", use_container_width=True)

            if submitted:
                if not seleccion:
                    st.warning("Debe seleccionar al menos un trámite.")
                    st.markdown("</div>", unsafe_allow_html=True)
                    return
                if not confirmar or frase.strip().upper() != "SINCRONIZAR":
                    st.warning("Falta confirmar y escribir SINCRONIZAR.")
                    st.markdown("</div>", unsafe_allow_html=True)
                    return

                resultados = []
                try:
                    for etiqueta in seleccion:
                        row = opciones[etiqueta]
                        try:
                            res = _sincronizar_item(row, username)
                            resultados.append({"DIG_TRAMITE": row["DIG_TRAMITE"], "ok": True, **res})
                        except Exception as exc:
                            _auditar_evento({
                                "evento": "SYNC_MENOR_EDAD_CC_ERROR",
                                "usuario": username,
                                "tramite": row.get("DIG_TRAMITE"),
                                "error": str(exc),
                            })
                            resultados.append({
                                "DIG_TRAMITE": row.get("DIG_TRAMITE"),
                                "ok": False,
                                "error": str(exc),
                            })
                except Exception as exc:
                    st.error(str(exc))
                    st.markdown("</div>", unsafe_allow_html=True)
                    return

                st.success("Proceso de sincronización finalizado.")
                st.dataframe(pd.DataFrame(resultados), use_container_width=True, hide_index=True)
                _construir_indice_destinos_por_year.clear()
                st.session_state.pop("audit_menor_rows", None)
                st.info("Presionar Buscar menores del mes para refrescar el reporte.")

    st.markdown("</div>", unsafe_allow_html=True)
