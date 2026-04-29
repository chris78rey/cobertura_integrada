# =========================
# Configuración persistente de la app
# =========================

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
CONFIG_FILE = CONFIG_DIR / "app_config.json"

_DEFAULT_CONFIG: dict[str, Any] = {
    "pdf_output_dir": "/home/crrb/coberturas_generadas/",
}


def _ensure_config_dir() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def leer_config() -> dict[str, Any]:
    _ensure_config_dir()

    if not CONFIG_FILE.exists():
        guardar_config(_DEFAULT_CONFIG)
        return dict(_DEFAULT_CONFIG)

    try:
        raw = CONFIG_FILE.read_text(encoding="utf-8")
        data = json.loads(raw)

        # Asegurar que todas las claves default existan
        for key, value in _DEFAULT_CONFIG.items():
            if key not in data:
                data[key] = value

        return data

    except (json.JSONDecodeError, OSError):
        guardar_config(_DEFAULT_CONFIG)
        return dict(_DEFAULT_CONFIG)


def guardar_config(data: dict[str, Any]) -> None:
    _ensure_config_dir()

    merged = dict(_DEFAULT_CONFIG)
    merged.update(data)

    CONFIG_FILE.write_text(
        json.dumps(merged, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def obtener_pdf_output_dir() -> Path:
    config = leer_config()
    raw = config.get("pdf_output_dir", _DEFAULT_CONFIG["pdf_output_dir"])
    return Path(raw).expanduser().resolve()


def validar_directorio_salida(ruta: str) -> dict:
    """
    Valida que la ruta sea absoluta, la crea si no existe
    y verifica permisos de escritura.

    Returns:
        {"ok": True, "path": Path} o {"ok": False, "error": str}
    """
    if not ruta or not ruta.strip():
        return {"ok": False, "error": "La ruta no puede estar vacía."}

    path = Path(ruta.strip()).expanduser().resolve()

    if not path.is_absolute():
        return {"ok": False, "error": "La ruta debe ser absoluta (ej: /home/...)."}

    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return {"ok": False, "error": f"No se pudo crear la carpeta: {exc}"}

    # Prueba de escritura
    test_file = path / ".write_test_cobertura"
    try:
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink()
    except OSError as exc:
        return {"ok": False, "error": f"Sin permisos de escritura en: {path}\nDetalle: {exc}"}

    return {"ok": True, "path": path}
