"""Rutas raíz del proyecto, configurables por entorno.

Por defecto apuntan a producción (/data_nuevo/...). Una copia de prueba (p. ej. una worktree de git)
define COBERTURA_PROJECT_ROOT / COBERTURA_OUTPUT_ROOT / COBERTURA_REPO_ROOT para tener su propio
estado, lock, logs, salida de PDFs y repositorio, sin tocar los de producción.
"""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("COBERTURA_PROJECT_ROOT") or "/data_nuevo/cobertura_integrada")
OUTPUT_ROOT = Path(os.environ.get("COBERTURA_OUTPUT_ROOT") or "/data_nuevo/coberturas")
REPO_ROOT = Path(os.environ.get("COBERTURA_REPO_ROOT") or "/data_nuevo/repo_grande/data/datos")
