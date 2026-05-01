# =========================
# Configuración persistente de la app
# Ruta de salida fija y protegida
# =========================

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
CONFIG_FILE = CONFIG_DIR / "app_config.json"

FIXED_PDF_OUTPUT_DIR = Path("/data_nuevo/coberturas").resolve()

_DEFAULT_CONFIG: dict[str, Any] = {
    "pdf_output_dir": str(FIXED_PDF_OUTPUT_DIR),
}


def _ensure_config_dir() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def _normalizar_config(data: dict[str, Any] | None = None) -> dict[str, Any]:
    base: dict[str, Any] = dict(data or {})
    base["pdf_output_dir"] = str(FIXED_PDF_OUTPUT_DIR)
    return base


def leer_config() -> dict[str, Any]:
    _ensure_config_dir()
    if not CONFIG_FILE.exists():
        config = _normalizar_config(_DEFAULT_CONFIG)
        guardar_config(config)
        return config
    try:
        raw = CONFIG_FILE.read_text(encoding="utf-8")
        data = json.loads(raw)
        config = _normalizar_config(data)
        if data.get("pdf_output_dir") != str(FIXED_PDF_OUTPUT_DIR):
            guardar_config(config)
        return config
    except (json.JSONDecodeError, OSError):
        config = _normalizar_config(_DEFAULT_CONFIG)
        guardar_config(config)
        return config


def guardar_config(data: dict[str, Any]) -> None:
    _ensure_config_dir()
    merged = dict(data or {})
    merged["pdf_output_dir"] = str(FIXED_PDF_OUTPUT_DIR)
    CONFIG_FILE.write_text(json.dumps(merged, indent=2, ensure_ascii=False), encoding="utf-8")


def obtener_pdf_output_dir() -> Path:
    FIXED_PDF_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return FIXED_PDF_OUTPUT_DIR


def validar_directorio_salida(ruta: str | None = None) -> dict:
    path = FIXED_PDF_OUTPUT_DIR
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return {"ok": False, "error": f"No se pudo crear la carpeta fija: {exc}"}
    test_file = path / ".write_test_cobertura"
    try:
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink()
    except OSError as exc:
        return {"ok": False, "error": f"Sin permisos de escritura en {path}\nDetalle: {exc}"}
    return {"ok": True, "path": path}
