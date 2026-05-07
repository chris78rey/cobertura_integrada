# =========================
# ORACLE JDBC CON FAILOVER + TIMEOUT DEFENSIVO
# =========================

from __future__ import annotations

import os
import time

# Forzar timezone Colombia para evitar ORA-01882
os.environ["TZ"] = "America/Bogota"
try:
    time.tzset()
except Exception:
    pass

import pandas as pd
import jaydebeapi

from src.config import get_jdbc_jar, get_oracle_targets


ORACLE_DRIVER = "oracle.jdbc.OracleDriver"


def oracle_connect(username: str, password: str):
    """
    Crea una conexión nueva a Oracle usando JDBC.

    Importante:
    - No se comparte la conexión entre usuarios.
    - No se cachea globalmente.
    - Se intenta nodo por nodo para soportar failover manual.
    """

    if not username or not password:
        raise RuntimeError("Usuario o contraseña vacíos")

    # Forzar timezone del JVM a Colombia (previene ORA-01882)
    try:
        import jpype
        if jpype.isJVMStarted():
            java_util_tz = jpype.JPackage("java").util.TimeZone
            java_util_tz.setDefault(java_util_tz.getTimeZone("America/Bogota"))
    except Exception:
        pass

    jar = get_jdbc_jar()
    targets = get_oracle_targets()

    last_error: Exception | None = None

    for host, port, sid in targets:
        url = f"jdbc:oracle:thin:@{host}:{port}:{sid}"

        try:
            conn = jaydebeapi.connect(
                ORACLE_DRIVER,
                url,
                [username, password],
                jars=[str(jar)],
            )
            return conn

        except Exception as exc:
            last_error = exc
            continue

    raise RuntimeError(
        f"No fue posible conectar a Oracle en ningún nodo. Último error: {last_error}"
    )


def test_login(username: str, password: str) -> dict:
    """
    Valida las credenciales contra Oracle.
    """

    conn = None

    try:
        conn = oracle_connect(username, password)
        cursor = conn.cursor()
        cursor.execute("SELECT USER FROM DUAL")
        row = cursor.fetchone()
        cursor.close()

        return {
            "ok": True,
            "db_user": row[0] if row else username.upper(),
            "error": None,
        }

    except Exception as exc:
        return {
            "ok": False,
            "db_user": None,
            "error": str(exc),
        }

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _java_resultset_to_dataframe(result_set) -> pd.DataFrame:
    """
    Convierte un ResultSet JDBC Java a pandas DataFrame.
    """

    metadata = result_set.getMetaData()
    column_count = metadata.getColumnCount()

    columns = []

    for idx in range(1, column_count + 1):
        label = metadata.getColumnLabel(idx)
        columns.append(str(label))

    rows = []

    while result_set.next():
        row = []

        for idx in range(1, column_count + 1):
            value = result_set.getObject(idx)

            if value is None:
                row.append(None)
            else:
                row.append(str(value))

        rows.append(row)

    return pd.DataFrame(rows, columns=columns)


def query_dataframe(
    username: str,
    password: str,
    sql: str,
    params: list | tuple | None = None,
    max_rows: int = 500,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    """
    Ejecuta una consulta SELECT con timeout defensivo.
    """

    if params:
        raise RuntimeError(
            "Esta versión con timeout JDBC directo no acepta parámetros todavía. "
            "Use SQL controlado desde plantillas."
        )

    clean_sql = sql.strip().rstrip(";")

    if not clean_sql.lower().startswith("select"):
        raise RuntimeError("Por seguridad, solo se permiten consultas SELECT.")

    conn = None
    statement = None
    result_set = None

    safe_sql = f"""
        SELECT *
        FROM (
            {clean_sql}
        )
        WHERE ROWNUM <= {int(max_rows)}
    """

    try:
        conn = oracle_connect(username, password)
        java_conn = conn.jconn

        statement = java_conn.createStatement()
        statement.setQueryTimeout(int(timeout_seconds))
        statement.setMaxRows(int(max_rows))

        result_set = statement.executeQuery(safe_sql)

        return _java_resultset_to_dataframe(result_set)

    except Exception as exc:
        message = str(exc)

        if "ORA-01013" in message or "cancel" in message.lower() or "timeout" in message.lower():
            raise RuntimeError(
                f"La consulta superó el timeout configurado de {timeout_seconds} segundos "
                "y fue cancelada o interrumpida por el driver JDBC."
            ) from exc

        raise

    finally:
        if result_set:
            try:
                result_set.close()
            except Exception:
                pass

        if statement:
            try:
                statement.close()
            except Exception:
                pass

        if conn:
            try:
                conn.close()
            except Exception:
                pass

def actualizar_cobertura_por_tramite(
    username: str,
    password: str,
    dig_tramite: str,
) -> dict:
    """
    Actualiza DIG_COBERTURA='S' usando DIG_TRAMITE como llave operativa.

    Seguridad:
    - Solo actualiza si DIG_COBERTURA sigue en 'N'.
    - Solo actualiza si DIG_PLANILLADO está en 'S'.
    - Exige exactamente 1 fila afectada.
    - Verifica inmediatamente después del COMMIT que quedó en 'S'.
    """

    conn = None
    prepared_statement = None
    before_statement = None
    before_result = None
    after_statement = None
    after_result = None

    dig_tramite = str(dig_tramite or "").strip()
    if not dig_tramite:
        return {
            "ok": False,
            "affected": 0,
            "verified": False,
            "already_closed": False,
            "error": "Sin DIG_TRAMITE. No se permite actualizar Oracle.",
            "criterio": "DIG_TRAMITE",
            "oracle_context": {},
            "before_rows": [],
            "after_rows": [],
        }

    context_sql = """
        SELECT
            USER,
            SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'),
            SYS_CONTEXT('USERENV', 'DB_NAME'),
            SYS_CONTEXT('USERENV', 'INSTANCE_NAME'),
            SYS_CONTEXT('USERENV', 'SERVER_HOST'),
            SYS_CONTEXT('USERENV', 'SERVICE_NAME')
        FROM DUAL
    """

    select_sql = """
        SELECT
            TO_CHAR(DIG_TRAMITE),
            TRIM(NVL(DIG_PLANILLADO, '')),
            TRIM(NVL(DIG_COBERTURA, 'N')),
            TRIM(TO_CHAR(FE_PLA_ANIOMES))
        FROM DIGITALIZACION.DIGITALIZACION
        WHERE TO_CHAR(DIG_TRAMITE) = ?
    """

    update_sql = """
        UPDATE DIGITALIZACION.DIGITALIZACION
        SET DIG_COBERTURA = 'S'
        WHERE TO_CHAR(DIG_TRAMITE) = ?
          AND NVL(TRIM(DIG_COBERTURA), 'N') = 'N'
          AND TRIM(DIG_PLANILLADO) = 'S'
    """
    oracle_context = {}
    before_rows = []
    after_rows = []

    try:
        conn = oracle_connect(username, password)
        java_conn = conn.jconn
        java_conn.setAutoCommit(False)

        try:
            before_statement = java_conn.prepareStatement(select_sql)
            before_statement.setString(1, dig_tramite)
            before_result = before_statement.executeQuery()
            while before_result.next():
                before_rows.append(
                    {
                        "dig_tramite": str(before_result.getString(1) or "").strip(),
                        "dig_planillado": str(before_result.getString(2) or "").strip(),
                        "dig_cobertura": str(before_result.getString(3) or "").strip(),
                        "fe_pla_aniomes": str(before_result.getString(4) or "").strip(),
                    }
                )

            context_statement = java_conn.prepareStatement(context_sql)
            try:
                context_result = context_statement.executeQuery()
                if context_result.next():
                    oracle_context = {
                        "db_user": str(context_result.getString(1) or "").strip(),
                        "current_schema": str(context_result.getString(2) or "").strip(),
                        "db_name": str(context_result.getString(3) or "").strip(),
                        "instance_name": str(context_result.getString(4) or "").strip(),
                        "server_host": str(context_result.getString(5) or "").strip(),
                        "service_name": str(context_result.getString(6) or "").strip(),
                    }
            finally:
                try:
                    context_statement.close()
                except Exception:
                    pass
        except Exception as exc:
            try:
                java_conn.rollback()
            except Exception:
                pass
            return {
                "ok": False,
                "affected": 0,
                "verified": False,
                "already_closed": False,
                "error": f"No se pudo leer Oracle antes del UPDATE: {exc}",
                "criterio": "DIG_TRAMITE",
                "oracle_context": oracle_context,
                "before_rows": before_rows,
                "after_rows": after_rows,
            }

        if len(before_rows) != 1:
            try:
                java_conn.rollback()
            except Exception:
                pass
            return {
                "ok": False,
                "affected": 0,
                "verified": False,
                "already_closed": False,
                "error": (
                    f"Antes del UPDATE se esperaba exactamente 1 fila para DIG_TRAMITE={dig_tramite}, "
                    f"pero se encontraron {len(before_rows)}."
                ),
                "criterio": "DIG_TRAMITE",
                "oracle_context": oracle_context,
                "before_rows": before_rows,
                "after_rows": after_rows,
            }

        before = before_rows[0]
        if before.get("dig_cobertura") == "S":
            try:
                java_conn.rollback()
            except Exception:
                pass
            return {
                "ok": True,
                "affected": 0,
                "verified": True,
                "already_closed": True,
                "error": "",
                "criterio": "DIG_TRAMITE",
                "oracle_context": oracle_context,
                "before_rows": before_rows,
                "after_rows": before_rows,
            }

        if before.get("dig_planillado") != "S":
            try:
                java_conn.rollback()
            except Exception:
                pass
            return {
                "ok": False,
                "affected": 0,
                "verified": False,
                "already_closed": False,
                "error": (
                    f"No se actualiza porque DIG_PLANILLADO={before.get('dig_planillado', '')} "
                    f"para DIG_TRAMITE={dig_tramite}."
                ),
                "criterio": "DIG_TRAMITE",
                "oracle_context": oracle_context,
                "before_rows": before_rows,
                "after_rows": after_rows,
            }

        prepared_statement = java_conn.prepareStatement(update_sql)
        prepared_statement.setString(1, dig_tramite)

        affected = int(prepared_statement.executeUpdate())

        if affected != 1:
            try:
                java_conn.rollback()
            except Exception:
                pass
            return {
                "ok": False,
                "affected": affected,
                "verified": False,
                "already_closed": False,
                "error": (
                    f"Actualización Oracle no confirmada. Se esperaba exactamente 1 fila para DIG_TRAMITE={dig_tramite}, "
                    f"pero se afectaron {affected}. Se aplicó rollback."
                ),
                "criterio": "DIG_TRAMITE",
                "oracle_context": oracle_context,
                "before_rows": before_rows,
                "after_rows": after_rows,
            }

        java_conn.commit()

        after_statement = java_conn.prepareStatement(select_sql)
        after_statement.setString(1, dig_tramite)
        after_result = after_statement.executeQuery()
        while after_result.next():
            after_rows.append(
                {
                    "dig_tramite": str(after_result.getString(1) or "").strip(),
                    "dig_planillado": str(after_result.getString(2) or "").strip(),
                    "dig_cobertura": str(after_result.getString(3) or "").strip(),
                    "fe_pla_aniomes": str(after_result.getString(4) or "").strip(),
                }
            )

        verified = (
            len(after_rows) == 1
            and after_rows[0].get("dig_cobertura") == "S"
            and after_rows[0].get("dig_planillado") == "S"
        )

        if not verified:
            return {
                "ok": False,
                "affected": affected,
                "verified": False,
                "already_closed": False,
                "error": (
                    f"El UPDATE hizo COMMIT con affected=1 para DIG_TRAMITE={dig_tramite}, "
                    "pero la verificación posterior no confirmó DIG_COBERTURA='S'."
                ),
                "criterio": "DIG_TRAMITE",
                "oracle_context": oracle_context,
                "before_rows": before_rows,
                "after_rows": after_rows,
            }

        return {
            "ok": True,
            "affected": affected,
            "verified": True,
            "already_closed": False,
            "error": "",
            "criterio": "DIG_TRAMITE",
            "oracle_context": oracle_context,
            "before_rows": before_rows,
            "after_rows": after_rows,
        }

    except Exception as exc:
        if conn:
            try:
                conn.jconn.rollback()
            except Exception:
                pass

        return {
            "ok": False,
            "affected": 0,
            "verified": False,
            "already_closed": False,
            "error": str(exc),
            "criterio": "DIG_TRAMITE",
            "oracle_context": oracle_context,
            "before_rows": before_rows,
            "after_rows": after_rows,
        }

    finally:
        if after_result:
            try:
                after_result.close()
            except Exception:
                pass

        if after_statement:
            try:
                after_statement.close()
            except Exception:
                pass

        if before_result:
            try:
                before_result.close()
            except Exception:
                pass

        if before_statement:
            try:
                before_statement.close()
            except Exception:
                pass

        if prepared_statement:
            try:
                prepared_statement.close()
            except Exception:
                pass

        if conn:
            try:
                conn.close()
            except Exception:
                pass


def obtener_tramites_en_cola(
    username: str,
    password: str,
    fe_pla_aniomes_desde: str,
    limite: int = 200,
    dig_tramite: str | None = None,
) -> list[dict]:
    """
    Devuelve los próximos trámites pendientes que entrarían a la cola de procesamiento.
    Solo lectura. No actualiza nada.
    """

    conn = oracle_connect(username, password)
    rows: list[dict] = []

    sql = """
        SELECT *
        FROM (
            SELECT
                TO_CHAR(d.DIG_ID_TRAMITE)      AS DIG_ID_TRAMITE,
                TO_CHAR(d.DIG_ID_GENERACION)   AS DIG_ID_GENERACION,
                TO_CHAR(d.DIG_TRAMITE)         AS DIG_TRAMITE,
                TRIM(d.DIG_CEDULA)             AS DIG_CEDULA,
                TRIM(TO_CHAR(d.FE_PLA_ANIOMES)) AS FE_PLA_ANIOMES,
                TRIM(NVL(d.DIG_PLANILLADO, '')) AS DIG_PLANILLADO,
                TRIM(NVL(d.DIG_COBERTURA, ''))  AS DIG_COBERTURA,
                TRIM(NVL(d.DIG_DEPENDIENTE_01, '')) AS DIG_DEPENDIENTE_01,
                TRIM(NVL(d.DIG_DEPENDIENTE_02, '')) AS DIG_DEPENDIENTE_02,
                TO_CHAR(d.DIG_FECHA_PLANILLA, 'YYYY-MM-DD HH24:MI:SS') AS DIG_FECHA_PLANILLA
            FROM DIGITALIZACION.DIGITALIZACION d
            WHERE TRIM(TO_CHAR(d.FE_PLA_ANIOMES)) >= ?
              AND NVL(TRIM(d.DIG_COBERTURA), 'N') = 'N'
              AND TRIM(d.DIG_PLANILLADO) = 'S'
              AND (? IS NULL OR TO_CHAR(d.DIG_TRAMITE) = ?)
            ORDER BY
                TRIM(TO_CHAR(d.FE_PLA_ANIOMES)),
                TO_CHAR(d.DIG_TRAMITE),
                TO_CHAR(d.DIG_ID_TRAMITE)
        )
        WHERE ROWNUM <= ?
    """

    pstmt = None
    rs = None

    try:
        pstmt = conn.jconn.prepareStatement(sql)
        pstmt.setString(1, str(fe_pla_aniomes_desde))
        if dig_tramite and str(dig_tramite).strip():
            tramite = str(dig_tramite).strip()
            pstmt.setString(2, tramite)
            pstmt.setString(3, tramite)
        else:
            pstmt.setNull(2, 12)  # VARCHAR
            pstmt.setNull(3, 12)  # VARCHAR
        pstmt.setInt(4, int(limite))

        try:
            pstmt.setQueryTimeout(20)
        except Exception:
            pass

        rs = pstmt.executeQuery()
        meta = rs.getMetaData()
        total_cols = meta.getColumnCount()

        while rs.next():
            row = {}
            for idx in range(1, total_cols + 1):
                col_name = meta.getColumnLabel(idx)
                value = rs.getString(idx)
                row[col_name] = value.strip() if isinstance(value, str) else value
            rows.append(row)

        return rows

    finally:
        try:
            if rs:
                rs.close()
        except Exception:
            pass
        try:
            if pstmt:
                pstmt.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# =========================
# FIN
# =========================
