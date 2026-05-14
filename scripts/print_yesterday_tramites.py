#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
sys.path.insert(0, str(PROJECT_ROOT))

from src.oracle_jdbc import oracle_connect  # noqa: E402


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")

    username = os.environ.get("ORACLE_AUTO_USER", "").strip()
    password = os.environ.get("ORACLE_AUTO_PASSWORD", "").strip()

    if not username or not password:
        print("Faltan ORACLE_AUTO_USER u ORACLE_AUTO_PASSWORD en .env", file=sys.stderr)
        return 1

    sql = """
        SELECT
            TO_CHAR(DIG_TRAMITE) AS DIG_TRAMITE,
            TO_CHAR(DIG_FECHA_HASTA, 'YYYY-MM-DD') AS DIG_FECHA_HASTA,
            TRIM(NVL(DIG_COBERTURA, '')) AS DIG_COBERTURA,
            TRIM(NVL(DIG_PLANILLADO, '')) AS DIG_PLANILLADO
        FROM DIGITALIZACION.DIGITALIZACION
        WHERE TRUNC(DIG_FECHA_HASTA) = TRUNC(SYSDATE) - 1
          AND TRIM(NVL(DIG_COBERTURA, '')) = 'N'
          AND TRIM(DIG_PLANILLADO) = 'S'
        ORDER BY TO_NUMBER(TO_CHAR(DIG_TRAMITE)) DESC
    """

    conn = None
    ps = None
    rs = None

    try:
        conn = oracle_connect(username, password)
        ps = conn.jconn.prepareStatement(sql)
        ps.setQueryTimeout(60)
        rs = ps.executeQuery()

        total = 0
        while rs.next():
            tramite = str(rs.getString(1) or "").strip()
            fecha = str(rs.getString(2) or "").strip()
            cobertura = str(rs.getString(3) or "").strip()
            planillado = str(rs.getString(4) or "").strip()
            total += 1
            print(
                f"{total:04d} | DIG_TRAMITE={tramite} | "
                f"DIG_FECHA_HASTA={fecha} | DIG_COBERTURA={cobertura} | DIG_PLANILLADO={planillado}"
            )

        print(f"\nTOTAL: {total}")
        return 0

    except Exception as exc:
        print(f"Error consultando Oracle: {exc}", file=sys.stderr)
        return 2

    finally:
        for obj in (rs, ps, conn):
            if obj:
                try:
                    obj.close()
                except Exception:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
