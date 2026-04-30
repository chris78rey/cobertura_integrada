from __future__ import annotations

import fcntl
import inspect
import os
from pathlib import Path
from typing import Any, Callable

from src.cobertura_pdf import generar_coberturas_automaticas_desde_mes


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
LOCK_PATH = PROJECT_ROOT / "logs" / "cobertura_generation.lock"


class ProcesoCoberturaYaEnEjecucion(RuntimeError):
    pass


class ArchivoLock:
    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self.file = None

    def __enter__(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.lock_path.open("w", encoding="utf-8")

        try:
            fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ProcesoCoberturaYaEnEjecucion(
                f"Ya existe un proceso de coberturas ejecutándose. Lock: {self.lock_path}"
            ) from exc

        self.file.write(f"pid={os.getpid()}\n")
        self.file.flush()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.file:
            try:
                fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
            finally:
                self.file.close()


def ejecutar_coberturas_con_lock(
    username: str,
    password: str,
    fe_pla_aniomes_desde: str,
    output_dir: str,
    dig_tramite: str = "",
    progress_callback: Callable[[int, int, dict[str, str]], None] | None = None,
) -> dict[str, Any]:
    """
    Ejecuta generación con lock global.

    Esto evita que:
    - Streamlit y el recuperador automático procesen al mismo tiempo.
    - Dos usuarios den clic y arranquen doble proceso.
    - systemd timer dispare otro proceso mientras uno sigue activo.
    """

    kwargs = {
        "username": username,
        "password": password,
        "fe_pla_aniomes_desde": fe_pla_aniomes_desde,
        "dig_tramite": dig_tramite,
        "output_dir": output_dir,
        "progress_callback": progress_callback,
    }

    firma = inspect.signature(generar_coberturas_automaticas_desde_mes)
    kwargs_filtrados = {
        key: value
        for key, value in kwargs.items()
        if key in firma.parameters
    }

    with ArchivoLock(LOCK_PATH):
        return generar_coberturas_automaticas_desde_mes(**kwargs_filtrados)
