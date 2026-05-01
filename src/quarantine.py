from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
DB_PATH = PROJECT_ROOT / "logs" / "cobertura_quarantine.sqlite"


def _get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quarantine (
            clave TEXT PRIMARY KEY,
            tramite TEXT NOT NULL,
            motivo TEXT NOT NULL,
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_quarantine_expires
        ON quarantine(expires_at)
        """
    )
    conn.commit()
    return conn


def poner_en_cuarentena(
    clave: str,
    tramite: str,
    motivo: str,
    duracion_segundos: int = 300,
) -> None:
    """Registra un trámite fallido en cuarentena. Default: 5 minutos."""
    now = time.time()
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO quarantine (clave, tramite, motivo, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(clave) DO UPDATE SET
            motivo = excluded.motivo,
            expires_at = excluded.expires_at,
            retry_count = quarantine.retry_count + 1
        """,
        (clave, tramite, motivo, now, now + duracion_segundos),
    )
    conn.commit()
    conn.close()


def obtener_claves_en_cuarentena() -> set[str]:
    conn = _get_conn()
    now = time.time()
    rows = conn.execute(
        "SELECT clave FROM quarantine WHERE expires_at > ?", (now,)
    ).fetchall()
    conn.close()
    return {row[0] for row in rows}


def limpiar_cuarentena_expirada() -> int:
    conn = _get_conn()
    now = time.time()
    cursor = conn.execute("DELETE FROM quarantine WHERE expires_at <= ?", (now,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return int(deleted or 0)


def contar_en_cuarentena() -> int:
    conn = _get_conn()
    now = time.time()
    row = conn.execute(
        "SELECT COUNT(*) FROM quarantine WHERE expires_at > ?", (now,)
    ).fetchone()
    conn.close()
    return int(row[0]) if row else 0


def listar_cuarentena_activa(limit: int = 50) -> list[dict[str, Any]]:
    """Devuelve registros bloqueados por cuarentena. Solo lectura."""
    conn = _get_conn()
    now = time.time()
    rows = conn.execute(
        """
        SELECT clave, tramite, motivo, created_at, expires_at, retry_count
        FROM quarantine WHERE expires_at > ?
        ORDER BY expires_at DESC LIMIT ?
        """,
        (now, int(limit)),
    ).fetchall()
    conn.close()

    resultado: list[dict[str, Any]] = []
    for row in rows:
        clave, tramite, motivo, created_at, expires_at, retry_count = row
        segundos_restantes = max(0, int(float(expires_at) - now))
        resultado.append({
            "clave": clave,
            "tramite": tramite,
            "motivo": motivo,
            "retry_count": retry_count,
            "segundos_restantes": segundos_restantes,
            "minutos_restantes": round(segundos_restantes / 60, 1),
            "created_at_local": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(created_at))),
            "expires_at_local": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(expires_at))),
        })
    return resultado


def expirar_cuarentena_activa() -> int:
    """Desactiva cuarentena activa sin borrar histórico."""
    conn = _get_conn()
    now = time.time()
    cursor = conn.execute("UPDATE quarantine SET expires_at = ? WHERE expires_at > ?", (now - 1, now))
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    return int(affected or 0)


def expirar_cuarentena_por_tramite(tramite: str) -> int:
    """Desactiva cuarentena para un trámite específico."""
    tramite = str(tramite or "").strip()
    if not tramite:
        return 0
    conn = _get_conn()
    now = time.time()
    cursor = conn.execute(
        "UPDATE quarantine SET expires_at = ? WHERE expires_at > ? AND tramite = ?",
        (now - 1, now, tramite),
    )
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    return int(affected or 0)


def segundos_hasta_proxima_expiracion(default_segundos: int = 30) -> int:
    """Cuántos segundos faltan para que expire la cuarentena más cercana."""
    conn = _get_conn()
    now = time.time()
    row = conn.execute("SELECT MIN(expires_at) FROM quarantine WHERE expires_at > ?", (now,)).fetchone()
    conn.close()
    if not row or row[0] is None:
        return int(default_segundos)
    return max(1, int(float(row[0]) - now))


def resumen_cuarentena_activa() -> dict[str, Any]:
    """Resumen liviano para logs y decisiones automáticas."""
    conn = _get_conn()
    now = time.time()
    row = conn.execute(
        "SELECT COUNT(*), MIN(expires_at), MAX(retry_count) FROM quarantine WHERE expires_at > ?",
        (now,),
    ).fetchone()
    conn.close()
    total = int(row[0] or 0) if row else 0
    proxima = row[1] if row else None
    max_reintentos = int(row[2] or 0) if row else 0
    segundos = int(float(proxima) - now) if proxima else 0
    return {
        "total": total,
        "segundos_hasta_proxima_expiracion": max(0, segundos),
        "max_reintentos": max_reintentos,
    }
