#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import unicodedata
from pathlib import Path


DEFAULT_ROOT = Path("/data_nuevo/coberturas")
DEFAULT_TERMS = (
    "servicio no disponible",
    "service unavailable",
    "503",
)


def _normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower()


def _extract_text_with_pdftotext(pdf_path: Path) -> str:
    cmd = ["pdftotext", "-layout", str(pdf_path), "-"]
    completed = subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return completed.stdout or ""


def _extract_text_fallback(pdf_path: Path) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception:
        try:
            from PyPDF2 import PdfReader  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "No se pudo leer el PDF. Instale 'pdftotext' o el paquete 'pypdf'."
            ) from exc

    reader = PdfReader(str(pdf_path))
    chunks: list[str] = []
    for page in reader.pages:
        try:
            chunks.append(page.extract_text() or "")
        except Exception:
            chunks.append("")
    return "\n".join(chunks)


def extract_text(pdf_path: Path) -> str:
    if shutil.which("pdftotext"):
        return _extract_text_with_pdftotext(pdf_path)
    return _extract_text_fallback(pdf_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Busca el primer PDF que contenga uno de los terminos indicados "
            "dentro de un directorio."
        )
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=str(DEFAULT_ROOT),
        help="Directorio raiz a recorrer. Por defecto: /data_nuevo/coberturas",
    )
    parser.add_argument(
        "terms",
        nargs="*",
        default=list(DEFAULT_TERMS),
        help=(
            "Terminos a buscar. Si no se especifican, usa "
            "'servicio no disponible', 'service unavailable' y '503'."
        ),
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if not root.exists():
        print(f"ROOT_NO_EXISTE|{root}", file=sys.stderr)
        return 2

    terms = [str(term).strip() for term in args.terms if str(term).strip()]
    if not terms:
        terms = list(DEFAULT_TERMS)

    normalized_terms = [_normalize_text(term) for term in terms]

    for pdf_path in sorted(root.rglob("*.pdf")):
        if not pdf_path.is_file():
            continue
        try:
            text = extract_text(pdf_path)
        except Exception as exc:
            print(f"ERROR_LEYENDO|{pdf_path}|{exc}", file=sys.stderr)
            continue

        normalized_text = _normalize_text(text)
        if any(term in normalized_text for term in normalized_terms):
            print(pdf_path)
            return 0

    print("NO_ENCONTRADO")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
