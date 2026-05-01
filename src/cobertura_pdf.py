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


def _validar_dig_tramite_opcional(value: str | None) -> str:
    """
    Valida DIG_TRAMITE opcional.
    Si viene vacío, no filtra por trámite.
    Si viene con valor, solo permite números para evitar filtros peligrosos.
    """
    tramite = str(value or "").strip()

    if not tramite:
        return ""

    if not re.fullmatch(r"\d{1,30}", tramite):
        raise RuntimeError(
            "DIG_TRAMITE inválido. Debe contener solo números. Ejemplo válido: 5899568."
        )

    return tramite


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


def _limitar_texto(value, max_chars=1200):
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n... TEXTO RECORTADO ..."


def _diagnosticar_fallo_pdf(
    result_node: dict,
    pdf_path: Path,
    cedula: str,
    fecha: str,
) -> dict:
    stdout = _limitar_texto(result_node.get("stdout", ""))
    stderr = _limitar_texto(result_node.get("stderr", ""))
    error = _limitar_texto(result_node.get("error", ""))

    texto = f"{error}\n{stderr}\n{stdout}".lower()

    if "timeout" in texto:
        causa = "La consulta tard\u00f3 demasiado y se agot\u00f3 el tiempo de espera."
        sugerencia = "Reintentar m\u00e1s tarde. Revisar si el portal est\u00e1 lento o si hay demasiada carga en el servidor."
        categoria = "TIMEOUT"
    elif "eacces" in texto or "permission denied" in texto or "permiso" in texto:
        causa = "La aplicaci\u00f3n no pudo escribir el archivo PDF en la carpeta destino."
        sugerencia = "Revisar permisos de escritura sobre la carpeta de salida y propietario del proceso Streamlit."
        categoria = "PERMISOS"
    elif "enoent" in texto or "no such file" in texto or "no existe" in texto:
        causa = "Falta una ruta, script, recurso o carpeta necesaria para generar el PDF."
        sugerencia = "Revisar COBERTURA_NODE_PROJECT_DIR, scripts/generate_pdf.js, assets y carpeta de salida."
        categoria = "RUTA_O_ARCHIVO_FALTANTE"
    elif "network" in texto or "fetch" in texto or "socket" in texto or "econn" in texto:
        causa = "Hubo un problema de red al consultar el portal."
        sugerencia = "Revisar conexi\u00f3n a internet, disponibilidad del portal y estabilidad de red."
        categoria = "RED_PORTAL"
    elif "captcha" in texto or "forbidden" in texto or "403" in texto or "429" in texto:
        causa = "El portal rechaz\u00f3 o limit\u00f3 temporalmente la consulta."
        sugerencia = "Bajar el ritmo de consultas, reintentar m\u00e1s tarde y revisar si el portal cambi\u00f3 su comportamiento."
        categoria = "PORTAL_LIMITO_CONSULTA"
    elif not pdf_path.exists():
        causa = "El generador termin\u00f3, pero el PDF esperado no apareci\u00f3 en la carpeta."
        sugerencia = "Revisar salida t\u00e9cnica de Node, nombre esperado del PDF, ruta de salida y assets del generador."
        categoria = "PDF_NO_CREADO"
    elif pdf_path.exists() and pdf_path.stat().st_size <= 0:
        causa = "El PDF fue creado, pero qued\u00f3 vac\u00edo."
        sugerencia = "Eliminar el PDF vac\u00edo y regenerar. Revisar respuesta del portal y plantilla de generaci\u00f3n."
        categoria = "PDF_VACIO"
    else:
        causa = "No se pudo determinar autom\u00e1ticamente la causa exacta."
        sugerencia = "Revisar el log t\u00e9cnico JSONL, stderr/stdout de Node y probar manualmente esa c\u00e9dula y fecha."
        categoria = "ERROR_DESCONOCIDO"

    return {
        "categoria": categoria,
        "causa": causa,
        "sugerencia": sugerencia,
        "cedula": cedula,
        "fecha": fecha,
        "pdf_esperado": str(pdf_path),
        "stdout": stdout,
        "stderr": stderr,
        "error_tecnico": error,
        "attempts": str(result_node.get("attempts", "")),
        "returncode": str(result_node.get("returncode", "")),
    }


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


def _validar_fe_pla_aniomes_desde_backend(valor: str) -> str:
    """
    Valida FE_PLA_ANIOMES desde backend.
    Evita valores vacíos, mal escritos o meses imposibles.
    Formato permitido: YYYYMM. Ejemplo: 202604.
    """
    mes = str(valor or "").strip()

    if not re.fullmatch(r"\d{6}", mes):
        raise ValueError(
            "FE_PLA_ANIOMES desde debe tener formato YYYYMM. Ejemplo válido: 202604."
        )

    numero_mes = int(mes[4:6])

    if numero_mes < 1 or numero_mes > 12:
        raise ValueError(
            "FE_PLA_ANIOMES tiene un mes inválido. Debe estar entre 01 y 12. Ejemplo válido: 202604."
        )

    return mes


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
                    "returncode": completed.returncode,
                    "cmd": " ".join(cmd),
                }

            last_error = (
                completed.stderr.strip()
                or completed.stdout.strip()
                or f"Node terminó con código {completed.returncode}"
            )
            last_stdout = completed.stdout
            last_stderr = completed.stderr
            last_returncode = completed.returncode

        except subprocess.TimeoutExpired:
            last_error = f"Timeout generando cobertura para {cedula} con fecha {fecha_pdf}"

        if attempt < max_retries:
            time.sleep(delay_seconds * attempt)

    return {
        "ok": False,
        "error": last_error,
        "stdout": last_stdout,
        "stderr": last_stderr,
        "returncode": last_returncode,
        "attempts": max_retries,
        "cmd": " ".join(cmd),
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
    fe_pla_aniomes_desde: str = "",
    dig_tramite: str = "",
    excluir_dig_id_tramite: set[str] | None = None,
    batch_size: int = 100,
    timeout_seconds: int = 300,
    fetch_size: int = 500,
) -> list[dict[str, str]]:
    """
    Obtiene solo un lote de registros pendientes.

    Importante:
    - No trae toda la tabla DIGITALIZACION.
    - Permite reconsultar Oracle varias veces durante el proceso.
    - Permite que entren registros nuevos mientras el proceso sigue vivo.
    - Excluye en esta corrida los DIG_ID_TRAMITE que ya fallaron para evitar ciclo infinito.
    """

    conn = None
    prepared_statement = None
    result_set = None

    dig_tramite = _validar_dig_tramite_opcional(dig_tramite)

    # INICIO NUEVO: exclusión segura de cuarentena
    excluir_dig_id_tramite = excluir_dig_id_tramite or set()

    excluir_lista_raw = [
        str(x).strip()
        for x in excluir_dig_id_tramite
        if str(x).strip()
    ]

    excluir_lista: list[str] = []
    vistos: set[str] = set()

    for item in excluir_lista_raw:
        if item not in vistos:
            excluir_lista.append(item)
            vistos.add(item)

    # Claves reales de DIG_ID_TRAMITE
    excluir_ids = [
        x for x in excluir_lista
        if not x.startswith("GEN_")
    ][:450]

    # Claves alternativas cuando DIG_ID_TRAMITE no existe
    excluir_gen = [
        x for x in excluir_lista
        if x.startswith("GEN_")
    ][:450]
    # FIN NUEVO

    sql = """
        SELECT *
        FROM (
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
            WHERE TRIM(TO_CHAR(FE_PLA_ANIOMES)) >= ?
              AND NVL(TRIM(DIG_COBERTURA), 'N') = 'N'
              AND TRIM(DIG_PLANILLADO) = 'S'
    """

    params: list[str | int] = [fe_pla_aniomes_desde]

    if dig_tramite:
        sql += """
              AND TO_CHAR(DIG_TRAMITE) = ?
        """
        params.append(dig_tramite)

    # INICIO NUEVO: no excluir accidentalmente registros con DIG_ID_TRAMITE NULL
    if excluir_ids:
        placeholders_ids = ",".join(["?"] * len(excluir_ids))

        sql += f"""
              AND (
                    DIG_ID_TRAMITE IS NULL
                    OR TRIM(TO_CHAR(DIG_ID_TRAMITE)) NOT IN ({placeholders_ids})
                  )
        """

        params.extend(excluir_ids)

    if excluir_gen:
        placeholders_gen = ",".join(["?"] * len(excluir_gen))

        sql += f"""
              AND (
                    DIG_ID_TRAMITE IS NOT NULL
                    OR (
                        'GEN_' || NVL(TRIM(TO_CHAR(DIG_ID_GENERACION)), '') NOT IN ({placeholders_gen})
                    )
                  )
        """

        params.extend(excluir_gen)
    # FIN NUEVO

    sql += """
            ORDER BY FE_PLA_ANIOMES, DIG_TRAMITE, DIG_ID_TRAMITE
        )
        WHERE ROWNUM <= ?
    """

    params.append(int(batch_size))

    registros: list[dict[str, str]] = []

    try:
        conn = oracle_connect(username, password)
        java_conn = conn.jconn

        prepared_statement = java_conn.prepareStatement(sql)

        for index, value in enumerate(params, start=1):
            if isinstance(value, int):
                prepared_statement.setInt(index, value)
            else:
                prepared_statement.setString(index, str(value))

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


def _contar_pendientes_automaticos(
    username: str,
    password: str,
    fe_pla_aniomes_desde: str,
    dig_tramite: str = "",
    timeout_seconds: int = 60,
) -> int:
    """
    Cuenta pendientes directamente desde la app.
    Sirve para comprobar que la aplicación ve los mismos pendientes que Oracle.
    """
    conn = None
    prepared_statement = None
    result_set = None

    sql = """
        SELECT COUNT(*)
        FROM DIGITALIZACION.DIGITALIZACION
        WHERE TRIM(TO_CHAR(FE_PLA_ANIOMES)) >= ?
          AND NVL(TRIM(DIG_COBERTURA), 'N') = 'N'
          AND TRIM(DIG_PLANILLADO) = 'S'
    """

    params = [fe_pla_aniomes_desde]

    if dig_tramite:
        sql += """
          AND TO_CHAR(DIG_TRAMITE) = ?
        """
        params.append(dig_tramite)

    try:
        conn = oracle_connect(username, password)
        java_conn = conn.jconn

        prepared_statement = java_conn.prepareStatement(sql)

        for index, value in enumerate(params, start=1):
            prepared_statement.setString(index, str(value))

        prepared_statement.setQueryTimeout(int(timeout_seconds))

        result_set = prepared_statement.executeQuery()

        if result_set.next():
            return int(result_set.getInt(1))

        return 0

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


def generar_coberturas_automaticas_desde_mes(
    username: str,
    password: str,
    fe_pla_aniomes_desde: str = "",
    dig_tramite: str = "",
    output_dir: str | Path | None = None,
    progress_callback: Callable[[int, int, dict[str, str]], None] | None = None,
    batch_size: int = 100,
    rondas_vacias_maximas: int = 3,
    espera_ronda_vacia_segundos: float = 5.0,
) -> dict[str, Any]:
    """
    Flujo automático:
    1. Consulta registros con FE_PLA_ANIOMES >= x, COBERTURA='N', PLANILLADO='S'
    2. Por cada fila: genera PDF de cobertura para titular y dependientes
    3. Solo si el PDF existe fÃ­sicamente, actualiza DIG_COBERTURA='S'
    4. Genera manifiesto CSV de auditorÃ­a

    No modifica DIG_PLANILLADO.
    """

    if not fe_pla_aniomes_desde:
        fe_pla_aniomes_desde = os.getenv("AUTO_FE_PLA_ANIOMES_DESDE", "202604")

    fe_pla_aniomes_desde = _validar_fe_pla_aniomes_desde_backend(
        fe_pla_aniomes_desde
    )

    dig_tramite = _validar_dig_tramite_opcional(dig_tramite)

    if output_dir is not None:
        output_root = Path(output_dir).expanduser().resolve()
        output_root.mkdir(parents=True, exist_ok=True)
    else:
        output_root = _get_output_root()
    node_project_dir = _get_node_project_dir()

    from src.oracle_jdbc import actualizar_cobertura_por_id_tramite
    from src.observability import RunLogger, build_run_id, mask_cedula
    from src.quarantine import (
        obtener_claves_en_cuarentena,
        poner_en_cuarentena,
        limpiar_cuarentena_expirada,
        contar_en_cuarentena,
        segundos_hasta_proxima_expiracion,
        resumen_cuarentena_activa,
    )

    run_id = build_run_id("cobertura_auto")
    logger = RunLogger(run_id)

    pendientes_previo = _contar_pendientes_automaticos(
        username=username,
        password=password,
        fe_pla_aniomes_desde=fe_pla_aniomes_desde,
        dig_tramite=dig_tramite,
    )

    logger.event(
        "DB_PRECOUNT_FINISHED",
        fe_pla_aniomes_desde=fe_pla_aniomes_desde,
        pendientes=pendientes_previo,
    )

    if progress_callback:
        progress_callback(
            0,
            max(pendientes_previo, 1),
            {
                "fe_pla_aniomes": fe_pla_aniomes_desde,
                "dig_tramite": "",
                "dig_cedula": "",
                "dig_fecha_hasta": "",
                "estado": f"Pendientes detectados por la aplicación: {pendientes_previo}",
            },
        )

    logger.event(
        "RUN_START",
        fe_pla_aniomes_desde=fe_pla_aniomes_desde,
        output_root=str(output_root),
        node_project_dir=str(node_project_dir),
    )

    logger.event(
        "DB_DYNAMIC_QUERY_MODE_START",
        fe_pla_aniomes_desde=fe_pla_aniomes_desde,
        dig_tramite=dig_tramite,
        batch_size=batch_size,
        rondas_vacias_maximas=rondas_vacias_maximas,
        espera_ronda_vacia_segundos=espera_ronda_vacia_segundos,
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filtro_nombre = f"tramite_{dig_tramite}" if dig_tramite else f"mes_{fe_pla_aniomes_desde}"
    manifest_path = output_root / f"manifest_cobertura_automatica_{filtro_nombre}_{timestamp}.csv"

    generados = 0
    actualizados = 0
    errores = 0
    errors_list: list[dict[str, str]] = []
    errores_consecutivos = 0
    procesados_global = 0
    lote_numero = 0
    rondas_vacias = 0
    dig_id_tramite_fallidos_en_corrida: set[str] = set()
    limpiar_cuarentena_expirada()
    cuarentena_inicial = contar_en_cuarentena()
    logger.event(
        "QUARANTINE_INITIAL",
        en_cuarentena=cuarentena_inicial,
    )

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
                "CEDULA_FALLIDA",
                "PDF_ESPERADO",
                "ERROR_CATEGORIA",
                "CAUSA_PROBABLE",
                "SUGERENCIA_REVISION",
                "NODE_ATTEMPTS",
                "NODE_RETURNCODE",
                "NODE_STDERR",
                "NODE_STDOUT",
                "FECHA_PROCESO",
            ],
        )
        writer.writeheader()

        while True:
            if _get_stop_flag().exists():
                logger.event(
                    "STOP_REQUEST_DETECTED_BEFORE_QUERY",
                    procesados_global=procesados_global,
                )
                break

            lote_numero += 1

            logger.event(
                "DB_BATCH_QUERY_START",
                lote_numero=lote_numero,
                procesados_global=procesados_global,
                fe_pla_aniomes_desde=fe_pla_aniomes_desde,
                dig_tramite=dig_tramite,
                excluidos_por_error=len(dig_id_tramite_fallidos_en_corrida),
            )

            try:
                # Combinar exclusión en memoria con cuarentena persistente
                exclusion_total = dig_id_tramite_fallidos_en_corrida | obtener_claves_en_cuarentena()

                registros = _obtener_registros_automaticos(
                    username=username,
                    password=password,
                    fe_pla_aniomes_desde=fe_pla_aniomes_desde,
                    dig_tramite=dig_tramite,
                    excluir_dig_id_tramite=exclusion_total,
                    batch_size=batch_size,
                )
            except Exception as exc:
                logger.error(
                    "DB_BATCH_QUERY_ERROR",
                    exc,
                    lote_numero=lote_numero,
                    procesados_global=procesados_global,
                    fe_pla_aniomes_desde=fe_pla_aniomes_desde,
                    dig_tramite=dig_tramite,
                )
                raise

            total = len(registros)

            if pendientes_previo > 0 and total == 0:
                en_cuarentena_actual = contar_en_cuarentena()
                excluidos_en_memoria = len(dig_id_tramite_fallidos_en_corrida)

                if en_cuarentena_actual > 0 or excluidos_en_memoria > 0:
                    resumen_q = resumen_cuarentena_activa()

                    espera_cuarentena = min(
                        max(1, segundos_hasta_proxima_expiracion(default_segundos=30)),
                        30,
                    )

                    logger.event(
                        "DB_ONLY_QUARANTINED_WAITING_AUTONOMOUS",
                        pendientes_oracle=pendientes_previo,
                        en_cuarentena=en_cuarentena_actual,
                        excluidos_en_memoria=excluidos_en_memoria,
                        espera_segundos=espera_cuarentena,
                        resumen_cuarentena=resumen_q,
                        mensaje=(
                            "Oracle reporta pendientes, pero los candidatos actuales están "
                            "temporalmente excluidos. El proceso esperará y volverá a consultar "
                            "sin intervención manual."
                        ),
                    )

                    if progress_callback:
                        progress_callback(
                            procesados_global,
                            max(pendientes_previo, 1),
                            {
                                "fe_pla_aniomes": fe_pla_aniomes_desde,
                                "dig_tramite": "",
                                "dig_cedula": "",
                                "dig_fecha_hasta": "",
                                "estado": (
                                    "Pendientes en cuarentena temporal. "
                                    f"Reintentando automáticamente en {espera_cuarentena}s..."
                                ),
                                "procesados_global": str(procesados_global),
                                "lote_numero": str(lote_numero),
                            },
                        )

                    inicio_espera_cuarentena = time.monotonic()

                    while time.monotonic() - inicio_espera_cuarentena < espera_cuarentena:
                        if _get_stop_flag().exists():
                            break
                        time.sleep(0.5)

                    limpiar_cuarentena_expirada()
                    dig_id_tramite_fallidos_en_corrida.clear()

                    if _get_stop_flag().exists():
                        break

                    continue

                raise RuntimeError(
                    "Inconsistencia crítica: Oracle reporta pendientes, pero la consulta de trabajo devolvió 0 registros. "
                    f"Pendientes detectados por la app: {pendientes_previo}. "
                    "Revisar ORACLE_TARGETS, usuario Oracle, filtro de trámite, FE_PLA_ANIOMES y versión/carpeta desde donde corre Streamlit."
                )

            if total == 0 and procesados_global == 0:
                return {
                    "ok": False,
                    "sin_pendientes": True,
                    "run_id": run_id,
                    "fe_pla_aniomes_desde": fe_pla_aniomes_desde,
                    "dig_tramite": dig_tramite,
                    "total": 0,
                    "generados": 0,
                    "actualizados": 0,
                    "errores": 0,
                    "output_root": str(output_root),
                    "manifest_path": str(manifest_path),
                    "errors": [],
                    "mensaje": "No se encontraron registros pendientes desde la aplicación.",
                }

            logger.event(
                "DB_BATCH_QUERY_FINISHED",
                lote_numero=lote_numero,
                total_lote=total,
                procesados_global=procesados_global,
            )

            if total == 0:
                rondas_vacias += 1

                logger.event(
                    "DB_EMPTY_ROUND",
                    ronda_vacia=rondas_vacias,
                    rondas_vacias_maximas=rondas_vacias_maximas,
                    espera_segundos=espera_ronda_vacia_segundos,
                    procesados_global=procesados_global,
                )

                if progress_callback:
                    progress_callback(
                        procesados_global,
                        max(procesados_global, 1),
                        {
                            "fe_pla_aniomes": "",
                            "dig_tramite": "",
                            "dig_cedula": "",
                            "dig_fecha_hasta": "",
                            "estado": (
                                f"Sin pendientes. Ronda vacía {rondas_vacias}/"
                                f"{rondas_vacias_maximas}. "
                                f"Esperando nuevos registros..."
                            ),
                            "procesados_global": str(procesados_global),
                            "lote_numero": str(lote_numero),
                        },
                    )

                if rondas_vacias >= rondas_vacias_maximas:
                    break

                inicio_espera_vacia = time.monotonic()
                while time.monotonic() - inicio_espera_vacia < espera_ronda_vacia_segundos:
                    if _get_stop_flag().exists():
                        break
                    time.sleep(0.5)

                continue

            rondas_vacias = 0

            for index, reg in enumerate(registros, start=1):
                procesados_global += 1

                # Usar clave compuesta si DIG_ID_TRAMITE está vacío
                dig_id_tramite = reg.get("dig_id_tramite", "").strip()
                dig_id_generacion = reg.get("dig_id_generacion", "").strip()
                clave_exclusion = dig_id_tramite if dig_id_tramite else f"GEN_{dig_id_generacion}"
                fe_pla = reg.get("fe_pla_aniomes", "")
                tramite = reg.get("dig_tramite", "")
                id_generacion = reg.get("dig_id_generacion", "")
                cedula = reg.get("dig_cedula", "")
                fecha_hasta = reg.get("dig_fecha_hasta", "")

                ultimo_segundos_pdf = 0.0
                espera = 0.0
                motivo_espera = "Sin espera calculada"

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
                error_en_pdf_detalle: dict[str, str] = {}

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
                        error_en_pdf_detalle = _diagnosticar_fallo_pdf(
                            result_node=result_node,
                            pdf_path=pdf_path,
                            cedula=c,
                            fecha=fecha_hasta,
                        )

                        error_en_pdf = error_en_pdf_detalle["error_tecnico"] or error_en_pdf_detalle["causa"]

                        logger.error(
                            "PDF_GENERATION_ERROR",
                            error_en_pdf,
                            index=index,
                            dig_tramite=tramite,
                            dig_id_tramite=dig_id_tramite,
                            cedula=mask_cedula(c),
                            tipo_persona=tipo_persona,
                            pdf_path=str(pdf_path),
                            segundos_pdf=round(ultimo_segundos_pdf, 3),
                            error_categoria=error_en_pdf_detalle["categoria"],
                            causa_probable=error_en_pdf_detalle["causa"],
                            sugerencia_revision=error_en_pdf_detalle["sugerencia"],
                            node_attempts=error_en_pdf_detalle["attempts"],
                            node_returncode=error_en_pdf_detalle["returncode"],
                            node_stderr=error_en_pdf_detalle["stderr"],
                            node_stdout=error_en_pdf_detalle["stdout"],
                        )
                        break

                # =========================
                # ACTUALIZACIÓN ORACLE POR TRÁMITE
                # Solo cuando TODOS los PDFs del trámite existen.
                # =========================
                todos_los_pdfs_ok = (
                    not error_en_pdf
                    and len(pdfs_generados) == total_pdfs_tramite
                    and all(p.exists() and p.stat().st_size > 0 for p in pdfs_generados)
                )

                if todos_los_pdfs_ok:
                    logger.event(
                        "ORACLE_UPDATE_START",
                        index=index,
                        dig_tramite=tramite,
                        dig_id_tramite=dig_id_tramite,
                        dig_id_generacion=id_generacion,
                        total_pdfs_tramite=total_pdfs_tramite,
                        pdfs_generados=len(pdfs_generados),
                        nuevo_valor="DIG_COBERTURA=S",
                    )

                    update_result = actualizar_cobertura_por_id_tramite(
                        username, password, dig_id_tramite,
                        dig_id_generacion=id_generacion,
                        dig_cedula=cedula,
                        dig_tramite=tramite,
                    )

                    logger.event(
                        "ORACLE_UPDATE_END",
                        index=index,
                        dig_tramite=tramite,
                        dig_id_tramite=dig_id_tramite,
                        dig_id_generacion=id_generacion,
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
                                "PDF_PATH": " | ".join(str(p) for p in pdfs_generados),
                                "PDF_SIZE_BYTES": sum(p.stat().st_size for p in pdfs_generados),
                                "ESTADO": "GENERADO_Y_ACTUALIZADO",
                                "PASO": "OK",
                                "ORACLE_AFFECTED": update_result.get("affected", 0),
                                "SEGUNDOS_PDF": round(ultimo_segundos_pdf, 3),
                                "ESPERA_SEGUNDOS": "",
                                "ERRORES_CONSECUTIVOS": "",
                                "ERROR": "",
                                "CEDULA_FALLIDA": "",
                                "PDF_ESPERADO": "",
                                "ERROR_CATEGORIA": "",
                                "CAUSA_PROBABLE": "",
                                "SUGERENCIA_REVISION": "",
                                "NODE_ATTEMPTS": "",
                                "NODE_RETURNCODE": "",
                                "NODE_STDERR": "",
                                "NODE_STDOUT": "",
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
                                    "procesados_global": str(procesados_global),
                                    "lote_numero": str(lote_numero),
                                },
                            )
                    else:
                        errores += 1
                        errores_consecutivos += 1
                        err_msg = update_result.get("error") or "No se pudo actualizar Oracle"

                        if clave_exclusion:
                            dig_id_tramite_fallidos_en_corrida.add(clave_exclusion)
                            poner_en_cuarentena(clave_exclusion, tramite, err_msg)

                        errors_list.append(
                            {
                                "dig_tramite": tramite,
                                "dig_id_tramite": dig_id_tramite,
                                "cedula": cedula,
                                "pdf_esperado": "",
                                "categoria": "ERROR_ACTUALIZANDO_ORACLE",
                                "causa": err_msg,
                                "sugerencia": "Revisar condición del UPDATE, DIG_ID_TRAMITE, DIG_ID_GENERACION, DIG_CEDULA y estado DIG_PLANILLADO.",
                                "error": err_msg,
                            }
                        )
                else:
                    errores += 1
                    errores_consecutivos += 1

                    if clave_exclusion:
                        dig_id_tramite_fallidos_en_corrida.add(clave_exclusion)
                        poner_en_cuarentena(clave_exclusion, tramite, err_msg or "PDFs incompletos")

                    err_msg = error_en_pdf or "No se generaron todos los PDFs esperados del trámite."

                    writer.writerow(
                        {
                            "RUN_ID": run_id,
                            "FE_PLA_ANIOMES": fe_pla,
                            "DIG_TRAMITE": tramite,
                            "DIG_ID_TRAMITE": dig_id_tramite,
                            "DIG_ID_GENERACION": id_generacion,
                            "DIG_CEDULA": cedula,
                            "DIG_FECHA_HASTA": fecha_hasta,
                            "PDF_PATH": " | ".join(str(p) for p in pdfs_generados),
                            "PDF_SIZE_BYTES": sum(p.stat().st_size for p in pdfs_generados) if pdfs_generados else 0,
                            "ESTADO": "NO_ACTUALIZADO_PDFS_INCOMPLETOS",
                            "PASO": "ERROR",
                            "ORACLE_AFFECTED": 0,
                            "SEGUNDOS_PDF": round(ultimo_segundos_pdf, 3),
                            "ESPERA_SEGUNDOS": "",
                            "ERRORES_CONSECUTIVOS": errores_consecutivos,
                            "ERROR": err_msg,
                            "CEDULA_FALLIDA": error_en_pdf_detalle.get("cedula", ""),
                            "PDF_ESPERADO": error_en_pdf_detalle.get("pdf_esperado", ""),
                            "ERROR_CATEGORIA": error_en_pdf_detalle.get("categoria", "PDFS_INCOMPLETOS"),
                            "CAUSA_PROBABLE": error_en_pdf_detalle.get("causa", err_msg),
                            "SUGERENCIA_REVISION": error_en_pdf_detalle.get("sugerencia", ""),
                            "NODE_ATTEMPTS": error_en_pdf_detalle.get("attempts", ""),
                            "NODE_RETURNCODE": error_en_pdf_detalle.get("returncode", ""),
                            "NODE_STDERR": error_en_pdf_detalle.get("stderr", ""),
                            "NODE_STDOUT": error_en_pdf_detalle.get("stdout", ""),
                            "FECHA_PROCESO": timestamp,
                        }
                    )

                if index < total:
                    try:
                        espera, motivo_espera = calcular_espera_dinamica(
                            output_root=output_root,
                            segundos_pdf=ultimo_segundos_pdf,
                            errores_consecutivos=errores_consecutivos,
                        )
                        espera = max(1.0, min(float(espera), 7.0))
                    except Exception as exc:
                        espera = 2.0
                        motivo_espera = "No se pudo calcular la espera din\u00e1mica. Se aplica espera segura de 2 segundos."
                        logger.error(
                            "THROTTLE_CALC_ERROR",
                            exc,
                            index=index,
                            dig_tramite=tramite,
                            dig_id_tramite=dig_id_tramite,
                            errores_consecutivos=errores_consecutivos,
                            ultimo_segundos_pdf=round(ultimo_segundos_pdf, 3),
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

                    if progress_callback:
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

                    # Espera cortada para que el bot\u00f3n de parar responda
                    inicio_espera = time.monotonic()
                    while time.monotonic() - inicio_espera < espera:
                        if _get_stop_flag().exists():
                            break
                        time.sleep(0.5)

                    if _get_stop_flag().exists():
                        logger.event(
                            "STOP_REQUEST_DETECTED_AFTER_ROW",
                            lote_numero=lote_numero,
                            procesados_global=procesados_global,
                        )
                        break

            # Fin del lote.
            # Se vuelve al while principal para consultar otra vez Oracle.
            # Aquí es donde entran los registros nuevos creados durante el proceso.
            if _get_stop_flag().exists():
                break

    logger.event(
        "RUN_END",
        total=procesados_global,
        generados=generados,
        actualizados=actualizados,
        errores=errores,
        manifest_path=str(manifest_path),
    )

    return {
        "ok": True,
        "run_id": run_id,
        "fe_pla_aniomes_desde": fe_pla_aniomes_desde,
        "dig_tramite": dig_tramite,
        "total": procesados_global,
        "generados": generados,
        "actualizados": actualizados,
        "errores": errores,
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "errors": errors_list,
    }
