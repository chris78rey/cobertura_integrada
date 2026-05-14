#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import shutil
import sys
import time
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
REPO_ROOT = Path("/data_nuevo/repo_grande/data/datos").resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from src.cobertura_pdf import (  # noqa: E402
    _expandir_cedulas_para_cobertura,
    _get_node_project_dir,
    _run_node_pdf_generator,
    _safe_name,
)
from src.oracle_jdbc import oracle_connect  # noqa: E402

PDF_CC_REGEX = re.compile(r"^CC(?:_\d{2})?\.pdf$", re.IGNORECASE)


def _exp_output_root() -> Path:
    raw = os.environ.get("COBERTURA_EXP_OUTPUT_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path("/data_nuevo/coberturas/prueba_loop").resolve()


def _es_pdf_cc(path: Path) -> bool:
    return path.is_file() and PDF_CC_REGEX.fullmatch(path.name) is not None


def _buscar_destino_existente_tramite(tramite: str) -> Path | None:
    if not REPO_ROOT.exists():
        return None

    matches: list[Path] = []
    for path in REPO_ROOT.rglob(tramite):
        if not path.is_dir() or path.name != tramite:
            continue
        resolved = path.resolve()
        if str(resolved).startswith(str(REPO_ROOT)):
            matches.append(resolved)

    matches = sorted(set(matches))
    if len(matches) == 1:
        return matches[0]
    return None


def _copiar_cc_a_destino_existente(origen_dir: Path, destino_dir: Path) -> tuple[int, list[str]]:
    if not origen_dir.exists() or not origen_dir.is_dir():
        return 0, ["ORIGEN_NO_EXISTE"]
    if not destino_dir.exists() or not destino_dir.is_dir():
        return 0, ["DESTINO_NO_EXISTE"]

    pdfs_cc = sorted([p for p in origen_dir.iterdir() if _es_pdf_cc(p)])
    if not pdfs_cc:
        return 0, ["SIN_PDFS_CC"]

    copiados = 0
    detalles: list[str] = []

    for pdf in pdfs_cc:
        if not pdf.exists() or pdf.stat().st_size <= 0:
            detalles.append(f"{pdf.name}:VACIO")
            continue

        dst_pdf = destino_dir / pdf.name
        shutil.copy2(pdf, dst_pdf)
        copiados += 1
        detalles.append(f"{pdf.name}:OK")

    return copiados, detalles


def _escribir_nota_local(planilla_dir: Path, tramite: str, mensaje: str, detalles: list[str] | None = None) -> Path:
    planilla_dir.mkdir(parents=True, exist_ok=True)
    nota_path = planilla_dir / "__NOTA_LOCAL.txt"
    lineas = [
        f"TRAMITE: {tramite}",
        f"MENSAJE: {mensaje}",
        f"TS: {time.strftime('%Y-%m-%d %H:%M:%S')}",
    ]
    if detalles:
        lineas.append("DETALLES:")
        for item in detalles:
            lineas.append(f"- {item}")
    lineas.append("")
    nota_path.write_text("\n".join(lineas), encoding="utf-8")
    return nota_path


def _escribir_marca_local(
    planilla_dir: Path,
    tramite: str,
    paso: bool,
    mensaje: str,
    detalles: list[str] | None = None,
) -> Path:
    archivo = planilla_dir / ("PASO.txt" if paso else "FALLO.txt")
    planilla_dir.mkdir(parents=True, exist_ok=True)
    lineas = [
        f"TRAMITE: {tramite}",
        f"RESULTADO: {'PASO' if paso else 'FALLO'}",
        f"MENSAJE: {mensaje}",
        f"TS: {time.strftime('%Y-%m-%d %H:%M:%S')}",
    ]
    if detalles:
        lineas.append("DETALLES:")
        for item in detalles:
            lineas.append(f"- {item}")
    lineas.append("")
    archivo.write_text("\n".join(lineas), encoding="utf-8")
    return archivo


def _leer_tramites_ayer(limit: int) -> list[dict[str, str]]:
    username = os.environ.get("ORACLE_AUTO_USER", "").strip()
    password = os.environ.get("ORACLE_AUTO_PASSWORD", "").strip()

    if not username or not password:
        raise RuntimeError("Faltan ORACLE_AUTO_USER u ORACLE_AUTO_PASSWORD en .env")

    sql = """
        SELECT
            TO_CHAR(DIG_TRAMITE) AS DIG_TRAMITE,
            TRIM(NVL(DIG_CEDULA, '')) AS DIG_CEDULA,
            TRIM(NVL(DIG_DEPENDIENTE_01, '')) AS DIG_DEPENDIENTE_01,
            TRIM(NVL(DIG_DEPENDIENTE_02, '')) AS DIG_DEPENDIENTE_02,
            TRIM(TO_CHAR(DIG_FECHA_HASTA, 'YYYY-MM-DD')) AS DIG_FECHA_HASTA,
            TRIM(TO_CHAR(DIG_FECHA_ALTA, 'YYYY-MM-DD')) AS DIG_FECHA_ALTA,
            TRIM(NVL(DIG_MENOR_EDAD, '')) AS DIG_MENOR_EDAD,
            TRIM(NVL(DIG_PLANILLADO, '')) AS DIG_PLANILLADO,
            TRIM(NVL(DIG_COBERTURA, '')) AS DIG_COBERTURA
        FROM DIGITALIZACION.DIGITALIZACION
        WHERE TRUNC(DIG_FECHA_HASTA) = TRUNC(SYSDATE) - 1
          AND TRIM(NVL(DIG_COBERTURA, '')) = 'N'
          AND TRIM(DIG_PLANILLADO) = 'S'
        ORDER BY TO_NUMBER(TO_CHAR(DIG_TRAMITE)) DESC
    """

    conn = None
    ps = None
    rs = None
    rows: list[dict[str, str]] = []

    try:
        conn = oracle_connect(username, password)
        ps = conn.jconn.prepareStatement(sql)
        ps.setQueryTimeout(60)
        rs = ps.executeQuery()

        while rs.next():
            rows.append(
                {
                    "dig_tramite": str(rs.getString(1) or "").strip(),
                    "dig_cedula": str(rs.getString(2) or "").strip(),
                    "dig_dependiente_01": str(rs.getString(3) or "").strip(),
                    "dig_dependiente_02": str(rs.getString(4) or "").strip(),
                    "dig_fecha_hasta": str(rs.getString(5) or "").strip(),
                    "dig_fecha_alta": str(rs.getString(6) or "").strip(),
                    "dig_menor_edad": str(rs.getString(7) or "").strip(),
                    "dig_planillado": str(rs.getString(8) or "").strip(),
                    "dig_cobertura": str(rs.getString(9) or "").strip(),
                }
            )
            if limit and len(rows) >= limit:
                break

        return rows

    finally:
        for obj in (rs, ps, conn):
            if obj:
                try:
                    obj.close()
                except Exception:
                    pass


def _leer_tramite_especifico(tramite: str) -> list[dict[str, str]]:
    tramite = str(tramite or "").strip()
    if not tramite:
        return []

    username = os.environ.get("ORACLE_AUTO_USER", "").strip()
    password = os.environ.get("ORACLE_AUTO_PASSWORD", "").strip()

    if not username or not password:
        raise RuntimeError("Faltan ORACLE_AUTO_USER u ORACLE_AUTO_PASSWORD en .env")

    sql = """
        SELECT
            TO_CHAR(DIG_TRAMITE) AS DIG_TRAMITE,
            TRIM(NVL(DIG_CEDULA, '')) AS DIG_CEDULA,
            TRIM(NVL(DIG_DEPENDIENTE_01, '')) AS DIG_DEPENDIENTE_01,
            TRIM(NVL(DIG_DEPENDIENTE_02, '')) AS DIG_DEPENDIENTE_02,
            TRIM(TO_CHAR(DIG_FECHA_HASTA, 'YYYY-MM-DD')) AS DIG_FECHA_HASTA,
            TRIM(TO_CHAR(DIG_FECHA_ALTA, 'YYYY-MM-DD')) AS DIG_FECHA_ALTA,
            TRIM(NVL(DIG_MENOR_EDAD, '')) AS DIG_MENOR_EDAD,
            TRIM(NVL(DIG_PLANILLADO, '')) AS DIG_PLANILLADO,
            TRIM(NVL(DIG_COBERTURA, '')) AS DIG_COBERTURA
        FROM DIGITALIZACION.DIGITALIZACION
        WHERE TO_CHAR(DIG_TRAMITE) = ?
    """

    conn = None
    ps = None
    rs = None
    rows: list[dict[str, str]] = []

    try:
        conn = oracle_connect(username, password)
        ps = conn.jconn.prepareStatement(sql)
        ps.setString(1, tramite)
        ps.setQueryTimeout(60)
        rs = ps.executeQuery()

        while rs.next():
            rows.append(
                {
                    "dig_tramite": str(rs.getString(1) or "").strip(),
                    "dig_cedula": str(rs.getString(2) or "").strip(),
                    "dig_dependiente_01": str(rs.getString(3) or "").strip(),
                    "dig_dependiente_02": str(rs.getString(4) or "").strip(),
                    "dig_fecha_hasta": str(rs.getString(5) or "").strip(),
                    "dig_fecha_alta": str(rs.getString(6) or "").strip(),
                    "dig_menor_edad": str(rs.getString(7) or "").strip(),
                    "dig_planillado": str(rs.getString(8) or "").strip(),
                    "dig_cobertura": str(rs.getString(9) or "").strip(),
                }
            )

        return rows

    finally:
        for obj in (rs, ps, conn):
            if obj:
                try:
                    obj.close()
                except Exception:
                    pass


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")

    pause_seconds = float(os.environ.get("COBERTURA_EXP_PAUSA_SEGUNDOS", "3.5") or "3.5")
    pause_seconds = max(0.0, pause_seconds)
    batch_raw = os.environ.get("COBERTURA_EXP_BATCH_SIZE", "10").strip()
    batch_size = int(batch_raw) if batch_raw.isdigit() and int(batch_raw) > 0 else 10
    extra_pause_raw = os.environ.get("COBERTURA_EXP_EXTRA_PAUSA_CIERRE_SEGUNDOS", "2").strip()
    extra_pause_seconds = float(extra_pause_raw) if extra_pause_raw else 2.0
    tramite_especifico = os.environ.get("COBERTURA_EXP_TRAMITE", "").strip()
    output_root = _exp_output_root()
    output_root.mkdir(parents=True, exist_ok=True)
    node_project_dir = _get_node_project_dir()

    print(f"Root experimental: {output_root}")
    print(f"Tamaño de lote: {batch_size}")
    print(f"Pausa entre carpetas: {pause_seconds:.1f}s")
    print(f"Pausa extra al llegar a 10: {extra_pause_seconds:.1f}s")
    print(f"Trámite específico: {tramite_especifico or '-'}")
    print(f"Node project dir: {node_project_dir}\n")

    procesados: set[str] = set()
    total = 0
    total_bloques = 0

    while True:
        if tramite_especifico:
            registros = _leer_tramite_especifico(tramite_especifico)
            tramite_especifico = ""
        else:
            registros = _leer_tramites_ayer(0)
        if not registros:
            print("No se encontraron más trámites de ayer pendientes.")
            break

        lote: list[dict[str, str]] = []
        for reg in registros:
            tramite = reg["dig_tramite"]
            if not tramite or tramite in procesados:
                continue
            lote.append(reg)
            if len(lote) >= batch_size:
                break

        if not lote:
            print("No hay trámites nuevos para procesar en esta reconsulta.")
            break

        total_bloques += 1
        print(f"\nBLOQUE {total_bloques:03d} | {len(lote)} trámite(s)")

        for indice_lote, reg in enumerate(lote, start=1):
            tramite = reg["dig_tramite"]
            if not tramite:
                continue

            procesados.add(tramite)
            total += 1

            planilla_dir = output_root / _safe_name(tramite)
            if planilla_dir.exists():
                shutil.rmtree(planilla_dir, ignore_errors=True)
            planilla_dir.mkdir(parents=True, exist_ok=True)

            cedulas_a_generar = _expandir_cedulas_para_cobertura(reg)
            if not cedulas_a_generar:
                print(f"{total:04d} | {tramite} | sin PDFs esperados")
                continue

            print(f"{total:04d} | {tramite} | {len(cedulas_a_generar)} PDF(s)")
            for secuencia_pdf, item_cedula in enumerate(cedulas_a_generar, start=1):
                cedula = item_cedula["cedula"]
                fecha_pdf = item_cedula.get("fecha_pdf", reg["dig_fecha_hasta"])
                output_name = f"CC_{secuencia_pdf:02d}" if len(cedulas_a_generar) > 1 else "CC"
                pdf_path = planilla_dir / f"{output_name}.pdf"

                result_node = _run_node_pdf_generator(
                    node_project_dir=node_project_dir,
                    cedula=cedula,
                    fecha_pdf=fecha_pdf,
                    output_dir=planilla_dir,
                    output_name=output_name,
                    single_timeout_seconds=int(os.environ.get("COBERTURA_NODE_TIMEOUT_SECONDS", "30") or "30"),
                    max_retries=int(os.environ.get("COBERTURA_NODE_MAX_RETRIES", "3") or "3"),
                    delay_seconds=float(os.environ.get("COBERTURA_NODE_RETRY_DELAY", "0.25") or "0.25"),
                    expected_pdf_path=pdf_path,
                )

                if result_node.get("ok") and pdf_path.exists() and pdf_path.stat().st_size > 0:
                    print(f"   - {pdf_path.name} OK")
                else:
                    print(
                        f"   - {pdf_path.name} ERROR | "
                        f"returncode={result_node.get('returncode')} | "
                        f"error={result_node.get('error')}"
                    )

            pdfs_generados = [p for p in sorted(planilla_dir.iterdir()) if _es_pdf_cc(p)]
            completos = all(p.exists() and p.stat().st_size > 0 for p in pdfs_generados) and len(pdfs_generados) == len(
                cedulas_a_generar
            )

            if completos:
                destino_dir = _buscar_destino_existente_tramite(tramite)
                if destino_dir is None:
                    print(f"   - DESTINO_NO_ENCONTRADO | no existe carpeta destino para {tramite}")
                    _escribir_marca_local(
                        planilla_dir,
                        tramite,
                        False,
                        "No existe carpeta destino espejo para copiar los CC*.pdf.",
                        ["DESTINO_NO_ENCONTRADO"],
                    )
                    _escribir_nota_local(
                        planilla_dir,
                        tramite,
                        "No existe carpeta destino espejo para copiar los CC*.pdf.",
                        ["DESTINO_NO_ENCONTRADO"],
                    )
                else:
                    copiados, detalles_sync = _copiar_cc_a_destino_existente(planilla_dir, destino_dir)
                    if copiados:
                        print(f"   - DESTINO_OK | {copiados} archivo(s) copiados en {destino_dir}")
                        _escribir_marca_local(
                            planilla_dir,
                            tramite,
                            True,
                            "PDFs completos y copiados al destino espejo.",
                            detalles_sync,
                        )
                    else:
                        print(f"   - DESTINO_SIN_COPIA | {' | '.join(detalles_sync)}")
                        _escribir_marca_local(
                            planilla_dir,
                            tramite,
                            False,
                            "No se pudo copiar al destino espejo.",
                            detalles_sync,
                        )
                        _escribir_nota_local(
                            planilla_dir,
                            tramite,
                            "No se pudo copiar al destino espejo.",
                            detalles_sync,
                        )
            else:
                print("   - DESTINO_NO_EJECUTADO | PDFs locales incompletos")
                _escribir_marca_local(
                    planilla_dir,
                    tramite,
                    False,
                    "La generación local quedó incompleta.",
                    [f"PDFS_ESPERADOS={len(cedulas_a_generar)}", f"PDFS_LOCALES={len(pdfs_generados)}"],
                )
                _escribir_nota_local(
                    planilla_dir,
                    tramite,
                    "La generación local quedó incompleta.",
                    [f"PDFS_ESPERADOS={len(cedulas_a_generar)}", f"PDFS_LOCALES={len(pdfs_generados)}"],
                )

            if pause_seconds > 0 and indice_lote < len(lote):
                time.sleep(pause_seconds)

        if pause_seconds > 0:
            sleep_after_batch = pause_seconds
            if len(lote) >= 10:
                sleep_after_batch += max(0.0, extra_pause_seconds)
            print(f"Reconsulta Oracle en {sleep_after_batch:.1f}s...")
            time.sleep(sleep_after_batch)

    print(f"\nTOTAL CARPETAS: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
