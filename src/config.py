# =========================
# Configuración general
# =========================

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def get_jdbc_jar() -> Path:
    jar = Path(os.environ.get("ORACLE_JDBC_JAR", "jdbc/ojdbc8.jar")).expanduser()

    if not jar.exists():
        raise RuntimeError(f"No se encontró el driver JDBC en: {jar}")

    return jar


def get_oracle_targets() -> list[tuple[str, int, str]]:
    raw_targets = os.environ.get("ORACLE_TARGETS", "").strip()

    if not raw_targets:
        raise RuntimeError("No está configurada la variable ORACLE_TARGETS")

    targets: list[tuple[str, int, str]] = []

    for item in raw_targets.split(","):
        item = item.strip()

        if not item:
            continue

        parts = item.split(":")

        if len(parts) != 3:
            raise RuntimeError(
                f"Target inválido: {item}. Formato esperado: host:puerto:sid"
            )

        host, port, sid = parts
        targets.append((host, int(port), sid))

    if not targets:
        raise RuntimeError("ORACLE_TARGETS no contiene nodos válidos")

    return targets


def get_max_workers() -> int:
    return int(os.environ.get("MAX_WORKERS", "8"))


def get_default_max_rows() -> int:
    return int(os.environ.get("DEFAULT_MAX_ROWS", "500"))

# =========================
# FIN
# =========================
