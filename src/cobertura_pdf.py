from __future__ import annotations

import csv
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from src.oracle_jdbc import oracle_connect


def _validate_id_generacion(value: str) -> str:
    clean_value = str(value or "").strip()

    if not clean_value:
        raise RuntimeError("Debe ingresar el ID_GENERACION.")

    if not re.match(r"^[A-Za-z0-9_.-]+$", clean_value):
        raise RuntimeError(
            "ID_GENERACION inválido. Solo se permiten letras, números, punto, guion y guion bajo."
        )

    return clean_value


def _safe_name(value: str) -> str:
    value = str(value or "").strip()
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return value.strip("._-") or "SIN_NOMBRE"


def _next_cc_output_name(
    output_dir: Path,
    used_names: set[str],
    overwrite: bool = False,
) -> str:
    """
    Devuelve el siguiente nombre disponible dentro de una carpeta:

    CC
    CC_01
    CC_02
    CC_03

    El generador Node agregará la extensión .pdf.
    """

    if overwrite:
        if "CC" not in used_names:
            used_names.add("CC")
            return "CC"

        index = 1
        while True:
            candidate = f"CC_{index:02d}"
            if candidate not in used_names:
                used_names.add(candidate)
                return candidate
            index += 1

    if "CC" not in used_names and not (output_dir / "CC.pdf").exists():
        used_names.add("CC")
        return "CC"

    index = 1

    while True:
        candidate = f"CC_{index:02d}"
        pdf_path = output_dir / f"{candidate}.pdf"

        if candidate not in used_names and not pdf_path.exists():
            used_names.add(candidate)
            return candidate

        index += 1


def _get_node_project_dir() -> Path:
    raw = os.environ.get("COBERTURA_NODE_PROJECT_DIR", "").strip()

    if not raw:
        raise RuntimeError(
            "Falta configurar COBERTURA_NODE_PROJECT_DIR en el .env. "
            "Debe apuntar a la carpeta del proyecto Node donde existe scripts/generate_pdf.js."
        )

    path = Path(raw).expanduser().resolve()

    if not path.exists():
        raise RuntimeError(f"No existe COBERTURA_NODE_PROJECT_DIR: {path}")

    script = path / "scripts" / "generate_pdf.js"

    if not script.exists():
        raise RuntimeError(f"No existe el generador PDF: {script}")

    return path


def _get_output_root() -> Path:
    raw = os.environ.get("COBERTURA_OUTPUT_DIR", "").strip()

    if raw:
        path = Path(raw).expanduser().resolve()
    else:
        path = Path.cwd() / "salida_coberturas"

    path.mkdir(parents=True, exist_ok=True)
    return path


def contar_registros_cobertura(
    username: str,
    password: str,
    id_generacion: str,
    timeout_seconds: int = 60,
) -> dict[str, Any]:
    id_generacion = _validate_id_generacion(id_generacion)

    conn = None
    prepared_statement = None
    result_set = None

    sql = """
        SELECT COUNT(1)
        FROM (
            SELECT TO_CHAR(d.dig_tramite) planilla,
                   d.dig_cedula cedula,
                   TO_CHAR(d.dig_fecha_planilla, 'YYYY-MM-DD') fecha_pdf
              FROM digitalizacion d
             WHERE d.dig_id_generacion = ?
               AND d.dig_fecha_planilla IS NOT NULL
               AND d.dig_planillado = 'S'
               AND d.dig_cedula IS NOT NULL

            UNION

            SELECT TO_CHAR(d.dig_tramite) planilla,
                   d.dig_dependiente_01 cedula,
                   TO_CHAR(d.dig_fecha_planilla, 'YYYY-MM-DD') fecha_pdf
              FROM digitalizacion d
             WHERE d.dig_id_generacion = ?
               AND d.dig_fecha_planilla IS NOT NULL
               AND d.dig_planillado = 'S'
               AND d.dig_dependiente_01 IS NOT NULL

            UNION

            SELECT TO_CHAR(d.dig_tramite) planilla,
                   d.dig_dependiente_02 cedula,
                   TO_CHAR(d.dig_fecha_planilla, 'YYYY-MM-DD') fecha_pdf
              FROM digitalizacion d
             WHERE d.dig_id_generacion = ?
               AND d.dig_fecha_planilla IS NOT NULL
               AND d.dig_planillado = 'S'
               AND d.dig_dependiente_02 IS NOT NULL
        )
    """

    try:
        conn = oracle_connect(username, password)
        java_conn = conn.jconn

        prepared_statement = java_conn.prepareStatement(sql)
        prepared_statement.setString(1, id_generacion)
        prepared_statement.setString(2, id_generacion)
        prepared_statement.setString(3, id_generacion)
        prepared_statement.setQueryTimeout(int(timeout_seconds))

        result_set = prepared_statement.executeQuery()

        total = 0
        if result_set.next():
            total = int(result_set.getLong(1))

        return {
            "ok": True,
            "id_generacion": id_generacion,
            "rows": total,
            "found": total > 0,
        }

    finally:
        if result_set:
            try:
                result_set.close()
            except Exception:
                pass

        if prepared_statement:
            try:
                prepared_statement.close()
            except Exception:
                pass

        if conn:
            try:
                conn.close()
            except Exception:
                pass


def obtener_registros_cobertura(
    username: str,
    password: str,
    id_generacion: str,
    timeout_seconds: int = 120,
    fetch_size: int = 1000,
) -> list[dict[str, str]]:
    id_generacion = _validate_id_generacion(id_generacion)

    conn = None
    prepared_statement = None
    result_set = None

    sql = """
        SELECT planilla,
               cedula,
               fecha_pdf,
               fecha_texto
        FROM (
            SELECT TO_CHAR(d.dig_tramite) planilla,
                   d.dig_cedula cedula,
                   TO_CHAR(d.dig_fecha_planilla, 'YYYY-MM-DD') fecha_pdf,
                   TO_CHAR(d.dig_fecha_planilla, 'DD-MM-YYYY') fecha_texto
              FROM digitalizacion d
             WHERE d.dig_id_generacion = ?
               AND d.dig_fecha_planilla IS NOT NULL
               AND d.dig_planillado = 'S'
               AND d.dig_cedula IS NOT NULL

            UNION

            SELECT TO_CHAR(d.dig_tramite) planilla,
                   d.dig_dependiente_01 cedula,
                   TO_CHAR(d.dig_fecha_planilla, 'YYYY-MM-DD') fecha_pdf,
                   TO_CHAR(d.dig_fecha_planilla, 'DD-MM-YYYY') fecha_texto
              FROM digitalizacion d
             WHERE d.dig_id_generacion = ?
               AND d.dig_fecha_planilla IS NOT NULL
               AND d.dig_planillado = 'S'
               AND d.dig_dependiente_01 IS NOT NULL

            UNION

            SELECT TO_CHAR(d.dig_tramite) planilla,
                   d.dig_dependiente_02 cedula,
                   TO_CHAR(d.dig_fecha_planilla, 'YYYY-MM-DD') fecha_pdf,
                   TO_CHAR(d.dig_fecha_planilla, 'DD-MM-YYYY') fecha_texto
              FROM digitalizacion d
             WHERE d.dig_id_generacion = ?
               AND d.dig_fecha_planilla IS NOT NULL
               AND d.dig_planillado = 'S'
               AND d.dig_dependiente_02 IS NOT NULL
        )
        ORDER BY planilla, cedula
    """

    registros: list[dict[str, str]] = []

    try:
        conn = oracle_connect(username, password)
        java_conn = conn.jconn

        prepared_statement = java_conn.prepareStatement(sql)
        prepared_statement.setString(1, id_generacion)
        prepared_statement.setString(2, id_generacion)
        prepared_statement.setString(3, id_generacion)
        prepared_statement.setQueryTimeout(int(timeout_seconds))
        prepared_statement.setFetchSize(int(fetch_size))

        result_set = prepared_statement.executeQuery()

        while result_set.next():
            registros.append(
                {
                    "planilla": str(result_set.getString(1) or "").strip(),
                    "cedula": str(result_set.getString(2) or "").strip(),
                    "fecha_pdf": str(result_set.getString(3) or "").strip(),
                    "fecha_texto": str(result_set.getString(4) or "").strip(),
                }
            )

        return registros

    finally:
        if result_set:
            try:
                result_set.close()
            except Exception:
                pass

        if prepared_statement:
            try:
                prepared_statement.close()
            except Exception:
                pass

        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _run_node_pdf_generator(
    node_project_dir: Path,
    cedula: str,
    fecha_pdf: str,
    output_dir: Path,
    output_name: str,
    single_timeout_seconds: int,
    max_retries: int,
    delay_seconds: float,
) -> dict[str, Any]:
    node_bin = os.environ.get("COBERTURA_NODE_BIN", "node").strip() or "node"

    cmd = [
        node_bin,
        "scripts/generate_pdf.js",
        "--cedula",
        cedula,
        "--fecha",
        fecha_pdf,
        "--output_name",
        output_name,
        "--output_dir",
        str(output_dir),
    ]

    last_error = ""

    for attempt in range(1, max_retries + 1):
        try:
            completed = subprocess.run(
                cmd,
                cwd=str(node_project_dir),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=int(single_timeout_seconds),
                check=False,
            )

            if completed.returncode == 0:
                return {
                    "ok": True,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                    "attempts": attempt,
                }

            last_error = (
                completed.stderr.strip()
                or completed.stdout.strip()
                or f"Node terminó con código {completed.returncode}"
            )

        except subprocess.TimeoutExpired:
            last_error = f"Timeout generando cobertura para {cedula} con fecha {fecha_pdf}"

        if attempt < max_retries:
            time.sleep(delay_seconds * attempt)

    return {
        "ok": False,
        "error": last_error,
        "attempts": max_retries,
    }


def generar_hojas_cobertura_por_id(
    username: str,
    password: str,
    id_generacion: str,
    overwrite: bool = False,
    oracle_timeout_seconds: int = 180,
    fetch_size: int = 1000,
    single_timeout_seconds: int = 120,
    delay_seconds: float = 2.0,
    max_retries: int = 3,
) -> dict[str, Any]:
    id_generacion = _validate_id_generacion(id_generacion)

    output_root = _get_output_root()
    node_project_dir = _get_node_project_dir()

    registros = obtener_registros_cobertura(
        username=username,
        password=password,
        id_generacion=id_generacion,
        timeout_seconds=oracle_timeout_seconds,
        fetch_size=fetch_size,
    )

    total_por_planilla: dict[str, int] = {}

    for registro in registros:
        planilla_key = _safe_name(registro["planilla"])
        total_por_planilla[planilla_key] = total_por_planilla.get(planilla_key, 0) + 1

    secuencia_por_planilla: dict[str, int] = {}

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    manifest_path = output_root / f"manifest_coberturas_{_safe_name(id_generacion)}_{timestamp}.csv"

    generated = 0
    skipped = 0
    failed = 0
    folders_created: set[str] = set()
    errors: list[dict[str, str]] = []

    with manifest_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "planilla",
                "cedula",
                "fecha",
                "carpeta",
                "pdf",
                "estado",
                "error",
            ],
        )
        writer.writeheader()

        for registro in registros:
            planilla = registro["planilla"]
            cedula = registro["cedula"]
            fecha_pdf = registro["fecha_pdf"]
            fecha_texto = registro["fecha_texto"]

            planilla_dir = output_root / _safe_name(planilla)
            planilla_dir.mkdir(parents=True, exist_ok=True)
            folders_created.add(str(planilla_dir))

            planilla_key = _safe_name(planilla)
            total_en_planilla = total_por_planilla.get(planilla_key, 1)

            if total_en_planilla > 1:
                secuencia_por_planilla[planilla_key] = secuencia_por_planilla.get(planilla_key, 0) + 1
                output_name = f"CC_{secuencia_por_planilla[planilla_key]:02d}"
            else:
                output_name = "CC"

            pdf_path = planilla_dir / f"{output_name}.pdf"

            if pdf_path.exists() and not overwrite:
                skipped += 1
                writer.writerow(
                    {
                        "planilla": planilla,
                        "cedula": cedula,
                        "fecha": fecha_texto,
                        "carpeta": str(planilla_dir),
                        "pdf": str(pdf_path),
                        "estado": "OMITIDO_YA_EXISTE",
                        "error": "",
                    }
                )
                continue

            result = _run_node_pdf_generator(
                node_project_dir=node_project_dir,
                cedula=cedula,
                fecha_pdf=fecha_pdf,
                output_dir=planilla_dir,
                output_name=output_name,
                single_timeout_seconds=single_timeout_seconds,
                max_retries=max_retries,
                delay_seconds=delay_seconds,
            )

            if result["ok"] and pdf_path.exists():
                generated += 1
                writer.writerow(
                    {
                        "planilla": planilla,
                        "cedula": cedula,
                        "fecha": fecha_texto,
                        "carpeta": str(planilla_dir),
                        "pdf": str(pdf_path),
                        "estado": "GENERADO",
                        "error": "",
                    }
                )
            else:
                failed += 1
                error_message = str(result.get("error") or "No se generó el PDF.")
                errors.append(
                    {
                        "planilla": planilla,
                        "cedula": cedula,
                        "fecha": fecha_texto,
                        "error": error_message,
                    }
                )
                writer.writerow(
                    {
                        "planilla": planilla,
                        "cedula": cedula,
                        "fecha": fecha_texto,
                        "carpeta": str(planilla_dir),
                        "pdf": str(pdf_path),
                        "estado": "ERROR",
                        "error": error_message,
                    }
                )

            time.sleep(delay_seconds)

    return {
        "ok": True,
        "id_generacion": id_generacion,
        "total": len(registros),
        "generated": generated,
        "skipped": skipped,
        "failed": failed,
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "folders": sorted(folders_created),
        "errors": errors,
    }
