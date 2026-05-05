#!/usr/bin/env python3
"""
Exporta errores de generación de coberturas a Excel.
Uso:
  python scripts/exportar_errores_excel.py                          # todos los archivos de error
  python scripts/exportar_errores_excel.py --run 20260505_163436    # una corrida específica
  python scripts/exportar_errores_excel.py --hoy                    # solo errores de hoy
  python scripts/exportar_errores_excel.py --output mis_errores.xlsx
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"
DEFAULT_OUTPUT = PROJECT_ROOT / "errores_coberturas.xlsx"

COLUMNAS = [
    "Fecha",
    "Run ID",
    "Trámite",
    "Cédula",
    "Tipo persona",
    "Categoría error",
    "Causa probable",
    "Error",
    "PDF esperado",
    "Segundos",
]


def _acortar_error(texto: str, max_len: int = 200) -> str:
    texto = str(texto or "").strip()
    if len(texto) > max_len:
        return texto[:max_len] + "…"
    return texto


def _extraer_filas(archivo: Path) -> list[dict]:
    filas: list[dict] = []
    if not archivo.exists():
        return filas
    with archivo.open("r", encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            try:
                obj = json.loads(linea)
            except json.JSONDecodeError:
                continue
            if obj.get("event") != "PDF_GENERATION_ERROR":
                continue
            ts = obj.get("ts", "")
            try:
                fecha = datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                fecha = ts
            filas.append(
                {
                    "Fecha": fecha,
                    "Run ID": obj.get("run_id", ""),
                    "Trámite": obj.get("dig_tramite", ""),
                    "Cédula": obj.get("cedula", ""),
                    "Tipo persona": obj.get("tipo_persona", ""),
                    "Categoría error": obj.get("error_categoria", ""),
                    "Causa probable": obj.get("causa_probable", ""),
                    "Error": _acortar_error(obj.get("error", "")),
                    "PDF esperado": obj.get("pdf_path", ""),
                    "Segundos": obj.get("segundos_pdf", ""),
                }
            )
    return filas


def main():
    parser = argparse.ArgumentParser(description="Exportar errores de cobertura a Excel")
    parser.add_argument("--run", help="ID de corrida (ej: 20260505_163436)")
    parser.add_argument("--hoy", action="store_true", help="Solo errores de hoy")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Archivo Excel de salida (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    patron = "cobertura_auto_*_errors.jsonl"
    archivos = sorted(LOGS_DIR.glob(patron))

    if args.run:
        archivos = [LOGS_DIR / f"cobertura_auto_{args.run}_errors.jsonl"]
        if not archivos[0].exists():
            print(f"No existe: {archivos[0]}")
            sys.exit(1)

    if args.hoy:
        hoy = date.today().strftime("%Y%m%d")
        archivos = [a for a in archivos if hoy in a.name]

    if not archivos:
        print("No se encontraron archivos de error.")
        sys.exit(0)

    todas: list[dict] = []
    for archivo in archivos:
        todas.extend(_extraer_filas(archivo))

    if not todas:
        print("No hay errores PDF_GENERATION_ERROR en los archivos seleccionados.")
        sys.exit(0)

    df = pd.DataFrame(todas, columns=COLUMNAS)
    df.sort_values("Fecha", inplace=True)

    salida = Path(args.output)
    df.to_excel(salida, index=False, sheet_name="Errores")
    print(f"✅ {len(df)} errores exportados → {salida}")


if __name__ == "__main__":
    main()
