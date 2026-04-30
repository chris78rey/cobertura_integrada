from __future__ import annotations

import csv
import os
import random
import re
import subprocess
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import psutil

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


def _nombre_cc_por_secuencia(indice: int, total: int) -> str:
    if total <= 1:
        return "CC"
    return f"CC_{indice:02d}"


def _crear_zip_coberturas(
    zip_path: Path,
    files: list[Path],
    base_root: Path,
) -> Path:
    """
    Crea un ZIP respetando la estructura de carpetas.

    Ejemplo:
    5827922/CC_01.pdf
    5827922/CC_02.pdf
    5827926/CC.pdf
    """

    zip_path.parent.mkdir(parents=True, exist_ok=True)

    unique_files: list[Path] = []
    seen: set[str] = set()

    for file_path in files:
        file_path = file_path.resolve()

        if not file_path.exists():
            continue

        key = str(file_path)

        if key in seen:
            continue

        seen.add(key)
        unique_files.append(file_path)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        for file_path in unique_files:
            arcname = file_path.relative_to(base_root.resolve())
            zip_file.write(file_path, arcname)

    return zip_path


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


STOP_FLAG = Path("config/stop_cobertura.flag")


def _get_stop_flag() -> Path:
    base = Path(__file__).resolve().parent.parent
    return base / "config" / "stop_cobertura.flag"


def proceso_debe_parar() -> bool:
    return _get_stop_flag().exists()


def medir_carga_sistema(output_root: Path) -> dict:
    cpu = psutil.cpu_percent(interval=0.3)
    memoria = psutil.virtual_memory().percent
    disco = psutil.disk_usage(str(output_root)).percent

    return {
        "cpu": cpu,
        "memoria": memoria,
        "disco": disco,
    }


def calcular_espera_dinamica(
    output_root: Path,
    segundos_pdf: float = 0,
    errores_consecutivos: int = 0,
) -> tuple[float, str]:
    """
    Calcula una espera din\u00e1mica, pero con l\u00edmite absoluto entre 1 y 7 segundos.

    Regla oficial:
    - Ritmo r\u00e1pido: 1 a 2 segundos.
    - Ritmo normal: 1 a 4 segundos.
    - Carga media: 4 a 5.5 segundos.
    - Carga alta o errores repetidos: 5.5 a 7 segundos.
    - Nunca superar 7 segundos.
    """
    carga = medir_carga_sistema(output_root)

    cpu = carga["cpu"]
    memoria = carga["memoria"]
    disco = carga["disco"]

    if disco >= 95:
        return random.uniform(6, 7), (
            f"Carga cr\u00edtica: disco {disco:.1f}%. "
            "Se mantiene ritmo muy conservador sin superar 7 segundos."
        )

    if cpu >= 90 or memoria >= 90 or errores_consecutivos >= 5:
        return random.uniform(5.5, 7), (
            f"Carga alta: CPU {cpu:.1f}%, RAM {memoria:.1f}%, "
            f"errores consecutivos {errores_consecutivos}. "
            "Se baja el ritmo dentro del l\u00edmite de 7 segundos."
        )

    if cpu >= 75 or memoria >= 80 or segundos_pdf >= 15 or errores_consecutivos >= 3:
        return random.uniform(4, 5.5), (
            f"Carga media: CPU {cpu:.1f}%, RAM {memoria:.1f}%, "
            f"\u00faltimo PDF {segundos_pdf:.1f}s. "
            "Se aplica espera moderada."
        )

    return random.uniform(1, 4), (
        f"Carga normal: CPU {cpu:.1f}%, RAM {memoria:.1f}%. "
        "Ritmo normal."
    )


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
    progress_callback: Callable[[int, int, dict[str, str]], None] | None = None,
    crear_zip: bool = True,
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
    zip_files: list[Path] = []

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

        total_registros = len(registros)

        for index, registro in enumerate(registros, start=1):
            planilla = registro["planilla"]
            cedula = registro["cedula"]
            fecha_pdf = registro["fecha_pdf"]
            fecha_texto = registro["fecha_texto"]

            if progress_callback:
                progress_callback(
                    index,
                    total_registros,
                    {
                        "planilla": planilla,
                        "cedula": cedula,
                        "fecha": fecha_texto,
                        "estado": "PROCESANDO",
                    },
                )

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
                zip_files.append(pdf_path)
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
                zip_files.append(pdf_path)
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

    zip_path = None

    if crear_zip and zip_files:
        zip_name = f"coberturas_{_safe_name(id_generacion)}_{timestamp}.zip"
        zip_path_obj = output_root / zip_name

        _crear_zip_coberturas(
            zip_path=zip_path_obj,
            files=zip_files,
            base_root=output_root,
        )

        zip_path = str(zip_path_obj)

    return {
        "ok": True,
        "id_generacion": id_generacion,
        "total": len(registros),
        "generated": generated,
        "skipped": skipped,
        "failed": failed,
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "run_log_path": logger.paths()["run_log_path"],
        "error_log_path": logger.paths()["error_log_path"],
        "zip_path": zip_path,
        "folders": sorted(folders_created),
        "errors": errors,
    }


def _obtener_registros_automaticos(
    username: str,
    password: str,
    fe_pla_aniomes_desde: str = "202604",
    timeout_seconds: int = 300,
    fetch_size: int = 5000,
) -> list[dict[str, str]]:
    conn = None
    prepared_statement = None
    result_set = None

    sql = """
        SELECT
            DIG_TRAMITE,
            TO_CHAR(DIG_FECHA_HASTA, 'YYYY-MM-DD') AS FECHA_HASTA,
            DIG_CEDULA,
            DIG_MENOR_EDAD,
            DIG_DEPENDIENTE_01,
            DIG_DEPENDIENTE_02,
            DIG_PLANILLADO,
            DIG_COBERTURA,
            DIG_ID_GENERACION,
            DIG_ID_TIPO,
            DIG_NUMERO_SOLICITUD,
            DIG_BLOQUEO_SGH,
            DIG_ID_TRAMITE,
            DIG_USUARIO,
            FE_PLA_ANIOMES
        FROM DIGITALIZACION.DIGITALIZACION
        WHERE FE_PLA_ANIOMES >= ?
          AND DIG_COBERTURA = 'N'
          AND DIG_PLANILLADO = 'S'
        ORDER BY FE_PLA_ANIOMES, DIG_TRAMITE, DIG_ID_TRAMITE
    """

    registros: list[dict[str, str]] = []

    try:
        conn = oracle_connect(username, password)
        java_conn = conn.jconn

        prepared_statement = java_conn.prepareStatement(sql)
        prepared_statement.setString(1, fe_pla_aniomes_desde)
        prepared_statement.setQueryTimeout(int(timeout_seconds))
        prepared_statement.setFetchSize(int(fetch_size))

        result_set = prepared_statement.executeQuery()

        while result_set.next():
            registros.append(
                {
                    "dig_tramite": str(result_set.getString(1) or "").strip(),
                    "dig_fecha_hasta": str(result_set.getString(2) or "").strip(),
                    "dig_cedula": str(result_set.getString(3) or "").strip(),
                    "dig_menor_edad": str(result_set.getString(4) or "").strip(),
                    "dig_dependiente_01": str(result_set.getString(5) or "").strip(),
                    "dig_dependiente_02": str(result_set.getString(6) or "").strip(),
                    "dig_planillado": str(result_set.getString(7) or "").strip(),
                    "dig_cobertura": str(result_set.getString(8) or "").strip(),
                    "dig_id_generacion": str(result_set.getString(9) or "").strip(),
                    "dig_id_tipo": str(result_set.getString(10) or "").strip(),
                    "dig_numero_solicitud": str(result_set.getString(11) or "").strip(),
                    "dig_bloqueo_sgh": str(result_set.getString(12) or "").strip(),
                    "dig_id_tramite": str(result_set.getString(13) or "").strip(),
                    "dig_usuario": str(result_set.getString(14) or "").strip(),
                    "fe_pla_aniomes": str(result_set.getString(15) or "").strip(),
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


def _expandir_cedulas_para_cobertura(registro: dict[str, str]) -> list[dict[str, str]]:
    """
    Dado un registro de DIGITALIZACION, devuelve una lista con las cédulas
    que necesitan cobertura: titular + dependientes si DIG_MENOR_EDAD='S'.
    """
    cedulas: list[dict[str, str]] = []

    titular = registro.get("dig_cedula", "").strip()
    if titular:
        cedulas.append({"cedula": titular, "tipo": "TITULAR"})

    if registro.get("dig_menor_edad", "").strip() == "S":
        d1 = registro.get("dig_dependiente_01", "").strip()
        d2 = registro.get("dig_dependiente_02", "").strip()

        if d1:
            cedulas.append({"cedula": d1, "tipo": "DEPENDIENTE_01"})

        if d2 and d2 != d1:
            cedulas.append({"cedula": d2, "tipo": "DEPENDIENTE_02"})

    return cedulas


def generar_coberturas_automaticas_desde_mes(
    username: str,
    password: str,
    fe_pla_aniomes_desde: str = "202604",
    output_dir: str | Path | None = None,
    progress_callback: Callable[[int, int, dict[str, str]], None] | None = None,
) -> dict[str, Any]:
    """
    Flujo automático:
    1. Consulta registros con FE_PLA_ANIOMES >= x, COBERTURA='N', PLANILLADO='S'
    2. Por cada fila: genera PDF de cobertura para titular y dependientes
    3. Solo si el PDF existe fÃ­sicamente, actualiza DIG_COBERTURA='S'
    4. Genera manifiesto CSV de auditorÃ­a

    No modifica DIG_PLANILLADO.
    """

    if output_dir is not None:
        output_root = Path(output_dir).expanduser().resolve()
        output_root.mkdir(parents=True, exist_ok=True)
    else:
        output_root = _get_output_root()
    node_project_dir = _get_node_project_dir()

    from src.oracle_jdbc import actualizar_cobertura_por_id_tramite
    from src.observability import RunLogger, build_run_id, mask_cedula

    run_id = build_run_id("cobertura_auto")
    logger = RunLogger(run_id)

    logger.event(
        "RUN_START",
        fe_pla_aniomes_desde=fe_pla_aniomes_desde,
        output_root=str(output_root),
        node_project_dir=str(node_project_dir),
    )

    logger.event(
        "DB_QUERY_START",
        fe_pla_aniomes_desde=fe_pla_aniomes_desde,
    )

    try:
        registros = _obtener_registros_automaticos(
            username=username,
            password=password,
            fe_pla_aniomes_desde=fe_pla_aniomes_desde,
        )

        logger.event(
            "DB_QUERY_FINISHED",
            total_registros=len(registros),
            fe_pla_aniomes_desde=fe_pla_aniomes_desde,
        )

    except Exception as exc:
        logger.error(
            "DB_QUERY_ERROR",
            exc,
            fe_pla_aniomes_desde=fe_pla_aniomes_desde,
        )
        raise

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    manifest_path = output_root / f"manifest_cobertura_automatica_{fe_pla_aniomes_desde}_{timestamp}.csv"

    total = len(registros)
    generados = 0
    actualizados = 0
    errores = 0
    errors_list: list[dict[str, str]] = []
    errores_consecutivos = 0

    with manifest_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "RUN_ID",
                "FE_PLA_ANIOMES",
                "DIG_TRAMITE",
                "DIG_ID_TRAMITE",
                "DIG_ID_GENERACION",
                "DIG_CEDULA",
                "DIG_FECHA_HASTA",
                "PDF_PATH",
                "PDF_SIZE_BYTES",
                "ESTADO",
                "PASO",
                "ORACLE_AFFECTED",
                "SEGUNDOS_PDF",
                "ESPERA_SEGUNDOS",
                "ERRORES_CONSECUTIVOS",
                "ERROR",
                "FECHA_PROCESO",
            ],
        )
        writer.writeheader()

        for index, reg in enumerate(registros, start=1):
            fe_pla = reg.get("fe_pla_aniomes", "")
            tramite = reg.get("dig_tramite", "")
            dig_id_tramite = reg.get("dig_id_tramite", "")
            id_generacion = reg.get("dig_id_generacion", "")
            cedula = reg.get("dig_cedula", "")
            fecha_hasta = reg.get("dig_fecha_hasta", "")

            ultimo_segundos_pdf = 0.0

            logger.event(
                "ROW_START",
                index=index,
                total=total,
                fe_pla_aniomes=fe_pla,
                dig_tramite=tramite,
                dig_id_tramite=dig_id_tramite,
                dig_id_generacion=id_generacion,
                dig_cedula=mask_cedula(cedula),
            )

            if progress_callback:
                progress_callback(
                    index,
                    total,
                    {
                        "fe_pla_aniomes": fe_pla,
                        "dig_tramite": tramite,
                        "dig_id_tramite": dig_id_tramite,
                        "dig_cedula": cedula,
                        "dig_fecha_hasta": fecha_hasta,
                        "estado": "INICIANDO",
                    },
                )

            # Expandir cédulas a generar (titular + dependientes)
            cedulas_a_generar = _expandir_cedulas_para_cobertura(reg)
            pdfs_generados: list[Path] = []
            error_en_pdf = ""

            # Carpeta por trámite
            planilla_dir = output_root / _safe_name(tramite)
            planilla_dir.mkdir(parents=True, exist_ok=True)

            cedulas_a_generar = _expandir_cedulas_para_cobertura(reg)
            total_pdfs_tramite = len(cedulas_a_generar)
            pdfs_generados: list[Path] = []
            error_en_pdf = ""

            for secuencia_pdf, item_cedula in enumerate(cedulas_a_generar, start=1):
                c = item_cedula["cedula"]
                tipo_persona = item_cedula.get("tipo", "")

                output_name = _nombre_cc_por_secuencia(
                    indice=secuencia_pdf,
                    total=total_pdfs_tramite,
                )

                pdf_path = planilla_dir / f"{output_name}.pdf"

                logger.event(
                    "PDF_GENERATION_START",
                    index=index,
                    fe_pla_aniomes=fe_pla,
                    dig_tramite=tramite,
                    dig_id_tramite=dig_id_tramite,
                    cedula=mask_cedula(c),
                    tipo_persona=tipo_persona,
                    output_name=output_name,
                    pdf_path=str(pdf_path),
                )

                if pdf_path.exists() and pdf_path.stat().st_size > 0:
                    pdfs_generados.append(pdf_path)
                    pdf_size = pdf_path.stat().st_size
                    logger.event(
                        "PDF_ALREADY_EXISTS",
                        index=index,
                        dig_tramite=tramite,
                        pdf_path=str(pdf_path),
                        pdf_size_bytes=pdf_size,
                    )
                    continue

                inicio_pdf = time.monotonic()

                result_node = _run_node_pdf_generator(
                    node_project_dir=node_project_dir,
                    cedula=c,
                    fecha_pdf=fecha_hasta,
                    output_dir=planilla_dir,
                    output_name=output_name,
                    single_timeout_seconds=120,
                    max_retries=2,
                    delay_seconds=1.0,
                )

                ultimo_segundos_pdf = time.monotonic() - inicio_pdf
                pdf_size = pdf_path.stat().st_size if pdf_path.exists() else 0

                if result_node["ok"] and pdf_path.exists() and pdf_size > 0:
                    pdfs_generados.append(pdf_path)
                    logger.event(
                        "PDF_GENERATION_END",
                        index=index,
                        dig_tramite=tramite,
                        dig_id_tramite=dig_id_tramite,
                        cedula=mask_cedula(c),
                        tipo_persona=tipo_persona,
                        ok=True,
                        pdf_exists=True,
                        pdf_size_bytes=pdf_size,
                        pdf_path=str(pdf_path),
                        segundos_pdf=round(ultimo_segundos_pdf, 3),
                    )
                else:
                    error_en_pdf = str(
                        result_node.get("error")
                        or f"No se generó correctamente {pdf_path.name} para cédula {c}"
                    )
                    logger.event(
                        "PDF_GENERATION_ERROR",
                        index=index,
                        dig_tramite=tramite,
                        dig_id_tramite=dig_id_tramite,
                        cedula=mask_cedula(c),
                        pdf_path=str(pdf_path),
                        segundos_pdf=round(ultimo_segundos_pdf, 3),
                        error=error_en_pdf,
                    )
                    break

                time.sleep(1.0)            # Actualizar Oracle solo si todos los PDFs se generaron
            if pdfs_generados and not error_en_pdf:
                logger.event(
                    "ORACLE_UPDATE_START",
                    index=index,
                    dig_tramite=tramite,
                    dig_id_tramite=dig_id_tramite,
                    nuevo_valor="DIG_COBERTURA=S",
                )

                update_result = actualizar_cobertura_por_id_tramite(
                    username, password, dig_id_tramite
                )

                logger.event(
                    "ORACLE_UPDATE_END",
                    index=index,
                    dig_tramite=tramite,
                    dig_id_tramite=dig_id_tramite,
                    ok=bool(update_result.get("ok")),
                    affected=update_result.get("affected", 0),
                    error=update_result.get("error", ""),
                )

                if update_result["ok"] and update_result["affected"] > 0:
                    generados += 1
                    actualizados += 1
                    errores_consecutivos = 0

                    writer.writerow(
                        {
                            "RUN_ID": run_id,
                            "FE_PLA_ANIOMES": fe_pla,
                            "DIG_TRAMITE": tramite,
                            "DIG_ID_TRAMITE": dig_id_tramite,
                            "DIG_ID_GENERACION": id_generacion,
                            "DIG_CEDULA": cedula,
                            "DIG_FECHA_HASTA": fecha_hasta,
                            "PDF_PATH": str(pdfs_generados[0]),
                            "PDF_SIZE_BYTES": pdf_size,
                            "ESTADO": "GENERADO_Y_ACTUALIZADO",
                            "PASO": "OK",
                            "ORACLE_AFFECTED": update_result.get("affected", 0),
                            "SEGUNDOS_PDF": round(ultimo_segundos_pdf, 3),
                            "ESPERA_SEGUNDOS": "",
                            "ERRORES_CONSECUTIVOS": "",
                            "ERROR": "",
                            "FECHA_PROCESO": timestamp,
                        }
                    )

                    if progress_callback:
                        progress_callback(
                            index,
                            total,
                            {
                                "fe_pla_aniomes": fe_pla,
                                "dig_tramite": tramite,
                                "dig_id_tramite": dig_id_tramite,
                                "dig_cedula": cedula,
                                "dig_fecha_hasta": fecha_hasta,
                                "estado": "GENERADO_Y_ACTUALIZADO",
                            },
                        )
                else:
                    errores += 1
                    errores_consecutivos += 1
                    err_msg = update_result.get("error") or "No se pudo actualizar Oracle"
                    errors_list.append(
                        {
                            "dig_tramite": tramite,
                            "dig_id_tramite": dig_id_tramite,
                            "cedula": cedula,
                            "error": err_msg,
                        }
                    )
                    writer.writerow(
                        {
                            "RUN_ID": run_id,
                            "FE_PLA_ANIOMES": fe_pla,
                            "DIG_TRAMITE": tramite,
                            "DIG_ID_TRAMITE": dig_id_tramite,
                            "DIG_ID_GENERACION": id_generacion,
                            "DIG_CEDULA": cedula,
                            "DIG_FECHA_HASTA": fecha_hasta,
                            "PDF_PATH": str(pdfs_generados[0]),
                            "PDF_SIZE_BYTES": pdf_size,
                            "ESTADO": "ERROR_ACTUALIZANDO_ORACLE",
                            "PASO": "ORACLE_UPDATE",
                            "ORACLE_AFFECTED": 0,
                            "SEGUNDOS_PDF": round(ultimo_segundos_pdf, 3),
                            "ESPERA_SEGUNDOS": "",
                            "ERRORES_CONSECUTIVOS": "",
                            "ERROR": err_msg,
                            "FECHA_PROCESO": timestamp,
                        }
                    )
            else:
                errores += 1
                errores_consecutivos += 1
                err_msg = error_en_pdf or "PDF_NO_EXISTE"
                errors_list.append(
                    {
                        "dig_tramite": tramite,
                        "dig_id_tramite": dig_id_tramite,
                        "cedula": cedula,
                        "error": err_msg,
                    }
                )
                writer.writerow(
                    {
                        "RUN_ID": run_id,
                        "FE_PLA_ANIOMES": fe_pla,
                        "DIG_TRAMITE": tramite,
                        "DIG_ID_TRAMITE": dig_id_tramite,
                        "DIG_ID_GENERACION": id_generacion,
                        "DIG_CEDULA": cedula,
                        "DIG_FECHA_HASTA": fecha_hasta,
                        "PDF_PATH": "",
                        "PDF_SIZE_BYTES": 0,
                        "ESTADO": "ERROR_GENERANDO_PDF",
                        "PASO": "PDF_GENERATION",
                        "ORACLE_AFFECTED": 0,
                        "SEGUNDOS_PDF": round(ultimo_segundos_pdf, 3),
                        "ESPERA_SEGUNDOS": "",
                        "ERRORES_CONSECUTIVOS": "",
                        "ERROR": err_msg,
                        "FECHA_PROCESO": timestamp,
                    }
                )

            # Pausa dinámica según carga del sistema
            espera, motivo_espera = calcular_espera_dinamica(
                output_root=output_root,
                segundos_pdf=ultimo_segundos_pdf,
                errores_consecutivos=errores_consecutivos,
            )

            logger.event(
                "THROTTLE_WAIT",
                index=index,
                dig_tramite=tramite,
                espera_segundos=round(espera, 2),
                motivo=motivo_espera,
                errores_consecutivos=errores_consecutivos,
                ultimo_segundos_pdf=round(ultimo_segundos_pdf, 3),
            )

            if index < total and progress_callback:
                progress_callback(
                    index,
                    total,
                    {
                        "fe_pla_aniomes": fe_pla,
                        "dig_tramite": tramite,
                        "dig_cedula": cedula,
                        "dig_fecha_hasta": fecha_hasta,
                        "estado": f"{motivo_espera} Esperando {espera:.1f}s antes del siguiente registro...",
                    },
                )

            # Espera cortada para que el botón de parar responda
            if index < total:
                inicio_espera = time.monotonic()
                while time.monotonic() - inicio_espera < espera:
                    if _get_stop_flag().exists():
                        break
                    time.sleep(0.5)

    logger.event(
        "RUN_END",
        total=total,
        generados=generados,
        actualizados=actualizados,
        errores=errores,
        manifest_path=str(manifest_path),
    )

    return {
        "ok": True,
        "run_id": run_id,
        "fe_pla_aniomes_desde": fe_pla_aniomes_desde,
        "total": total,
        "generados": generados,
        "actualizados": actualizados,
        "errores": errores,
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "errors": errors_list,
    }
