# =========================
# ORACLE JDBC CON FAILOVER + TIMEOUT DEFENSIVO
# =========================

from __future__ import annotations

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

def actualizar_cobertura_por_id_tramite(
    username: str,
    password: str,
    dig_id_tramite: str,
    dig_id_generacion: str = "",
    dig_cedula: str = "",
) -> dict:
    """
    Actualiza DIG_COBERTURA='S' solo si está en 'N' y DIG_PLANILLADO='S'.

    Si DIG_ID_TRAMITE está vacío, usa DIG_ID_GENERACION + DIG_CEDULA
    como clave alternativa para identificar la fila.
    """

    conn = None
    prepared_statement = None

    dig_id_tramite = str(dig_id_tramite or "").strip()
    dig_id_generacion = str(dig_id_generacion or "").strip()
    dig_cedula = str(dig_cedula or "").strip()

    if dig_id_tramite:
        sql = """
            UPDATE DIGITALIZACION.DIGITALIZACION
            SET DIG_COBERTURA = 'S'
            WHERE DIG_ID_TRAMITE = ?
              AND NVL(TRIM(DIG_COBERTURA), 'N') = 'N'
              AND TRIM(DIG_PLANILLADO) = 'S'
        """
        params = [dig_id_tramite]
    elif dig_id_generacion and dig_cedula:
        sql = """
            UPDATE DIGITALIZACION.DIGITALIZACION
            SET DIG_COBERTURA = 'S'
            WHERE DIG_ID_GENERACION = ?
              AND DIG_CEDULA = ?
              AND (DIG_ID_TRAMITE IS NULL OR TRIM(DIG_ID_TRAMITE) IS NULL)
              AND NVL(TRIM(DIG_COBERTURA), 'N') = 'N'
              AND TRIM(DIG_PLANILLADO) = 'S'
        """
        params = [dig_id_generacion, dig_cedula]
    else:
        return {
            "ok": False,
            "affected": 0,
            "error": "Sin clave suficiente para actualizar (DIG_ID_TRAMITE y DIG_ID_GENERACION/CEDULA vacíos).",
        }

    try:
        conn = oracle_connect(username, password)
        java_conn = conn.jconn
        java_conn.setAutoCommit(False)

        prepared_statement = java_conn.prepareStatement(sql)

        for index, value in enumerate(params, start=1):
            prepared_statement.setString(index, str(value))

        affected = prepared_statement.executeUpdate()

        java_conn.commit()

        return {
            "ok": True,
            "affected": affected,
            "error": None,
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
            "error": str(exc),
        }

    finally:
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


# =========================
# FIN
# =========================
