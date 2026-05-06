import html
import json
import os
import re
import subprocess
import sys
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import streamlit as st

from src.app_config import (
    leer_config,
    guardar_config,
    obtener_pdf_output_dir,
    validar_directorio_salida,
)
from src.cobertura_pdf import (
    _contar_pendientes_automaticos,
)
from src.auto_resume_state import (
    leer_estado_job,
    _parse_ts,
    guardar_estado_job,
    registrar_job_activo,
    marcar_job_completado,
    marcar_job_reintento,
    marcar_job_detenido_por_usuario,
    marcar_job_vigilando_sin_pendientes,
)
from src.operator_tools import (
    leer_estado_operador,
    destrabar_para_reintento,
    pausar_reintento_automatico,
    exportar_estado_json,
)
from src.oracle_jdbc import obtener_tramites_en_cola


def _reset_all():
    for key in [
        "current_result",
        "current_error",
        "input_reset_counter",
    ]:
        if key in st.session_state:
            del st.session_state[key]

    st.session_state.input_reset_counter = st.session_state.get("input_reset_counter", 0) + 1


def _limpiar_bandera_parada():
    flag = Path(__file__).resolve().parent.parent.parent / "config" / "stop_cobertura.flag"
    if flag.exists():
        flag.unlink()


def _crear_bandera_parada():
    flag = Path(__file__).resolve().parent.parent.parent / "config" / "stop_cobertura.flag"
    flag.parent.mkdir(parents=True, exist_ok=True)
    flag.write_text("STOP", encoding="utf-8")


def _ruta_script_worker() -> Path:
    return Path(__file__).resolve().parent.parent.parent / "scripts" / "run_resume_coberturas.sh"


def _disparar_worker_inmediato() -> dict:
    """Lanza una ejecución inmediata del worker autónomo en segundo plano."""
    script = _ruta_script_worker()
    logs_dir = Path(__file__).resolve().parent.parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    launcher_log = logs_dir / "worker_launcher.log"

    if not script.exists():
        return {"ok": False, "error": f"No existe el script: {script}"}

    try:
        with launcher_log.open("a", encoding="utf-8") as fh:
            proc = subprocess.Popen(
                ["bash", str(script)],
                cwd=str(script.parent.parent),
                stdout=fh,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=os.environ.copy(),
            )
        return {"ok": True, "pid": proc.pid, "log_path": str(launcher_log)}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _init_state():
    if "input_reset_counter" not in st.session_state:
        st.session_state.input_reset_counter = 0

    if "current_result" not in st.session_state:
        st.session_state.current_result = None

    if "current_error" not in st.session_state:
        st.session_state.current_error = None


def _segundos_desde(ts: str | None) -> int | None:
    dt = _parse_ts(ts)
    if not dt:
        return None
    return max(0, int((datetime.now(timezone.utc) - dt).total_seconds()))


def _estado_semaforo(seconds_progress: int | None, sync_active: bool, status: str) -> tuple[str, str]:
    if sync_active:
        return "ok", "Sync en curso con heartbeat activo."
    if status in {"WATCHING_NO_PENDING", "COMPLETED", "STOPPED_BY_USER"}:
        return "ok", "Sin trabajo activo."
    if seconds_progress is None:
        return "warn", "Todavía no hay heartbeat suficiente para medir progreso."
    if seconds_progress < 300:
        return "ok", "Proceso con actividad reciente."
    if seconds_progress < 1200:
        return "warn", "Proceso lento o con pausas; sigue vivo."
    return "alert", "Proceso estancado o sin heartbeat reciente."


def _render_css():
    st.markdown(
        """
        <style>
        .block-container {
            max-width: 860px;
            padding-top: 1.5rem;
        }

        section[data-testid="stSidebar"] {
            display: none;
        }

        .main-title {
            text-align: center;
            font-size: 2rem;
            font-weight: 850;
            color: #0f172a;
            margin-bottom: 0.25rem;
        }

        .main-subtitle {
            text-align: center;
            color: #64748b;
            font-size: 1rem;
            margin-bottom: 1.5rem;
        }

        .simple-card {
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 24px;
            padding: 1.6rem;
            box-shadow: 0 14px 38px rgba(15, 23, 42, 0.08);
            margin-bottom: 1.2rem;
        }

        .config-label {
            font-weight: 700;
            font-size: 0.95rem;
            color: #0f172a;
            margin-bottom: 0.3rem;
        }

        .status-success {
            background: #ecfdf5;
            border: 1px solid #bbf7d0;
            color: #166534;
            border-radius: 18px;
            padding: 1rem;
            text-align: center;
            font-weight: 850;
            margin-top: 1rem;
            margin-bottom: 1rem;
        }

        .status-info {
            background: #eff6ff;
            border: 1px solid #bfdbfe;
            color: #1d4ed8;
            border-radius: 18px;
            padding: 1rem;
            text-align: center;
            font-weight: 800;
            margin-top: 1rem;
            margin-bottom: 1rem;
        }

        .status-warn {
            background: #fff7ed;
            border: 1px solid #fed7aa;
            color: #9a3412;
            border-radius: 18px;
            padding: 1rem;
            font-weight: 750;
            text-align: center;
            margin-top: 1rem;
            margin-bottom: 1rem;
        }

        .status-card {
            border-radius: 20px;
            padding: 1rem 1.1rem;
            margin: 0.5rem 0 1rem 0;
            border: 1px solid transparent;
            font-weight: 800;
        }

        .status-card.ok {
            background: #ecfdf5;
            border-color: #86efac;
            color: #166534;
        }

        .status-card.warn {
            background: #fffbeb;
            border-color: #fcd34d;
            color: #92400e;
        }

        .status-card.alert {
            background: #fef2f2;
            border-color: #fca5a5;
            color: #991b1b;
        }

        .op-summary-grid {
            margin: 0.5rem 0 1rem 0;
        }

        .op-card {
            border-radius: 24px;
            padding: 1rem 1.05rem;
            min-height: 220px;
            box-shadow: 0 16px 34px rgba(15, 23, 42, 0.08);
        }

        .op-card-primary {
            background: linear-gradient(180deg, #eff6ff 0%, #dbeafe 100%);
            border: 1px solid #93c5fd;
        }

        .op-card-accent {
            background: linear-gradient(180deg, #fff7ed 0%, #ffedd5 100%);
            border: 1px solid #fdba74;
        }

        .op-card-title {
            font-size: 0.82rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            font-weight: 900;
            color: #334155;
            margin-bottom: 0.8rem;
        }

        .op-card-row {
            display: flex;
            justify-content: space-between;
            gap: 0.8rem;
            padding: 0.35rem 0;
            border-bottom: 1px solid rgba(148, 163, 184, 0.22);
        }

        .op-card-row:last-child {
            border-bottom: none;
        }

        .op-card-row span {
            color: #475569;
            font-size: 0.88rem;
        }

        .op-card-row strong {
            color: #0f172a;
            font-size: 0.92rem;
            text-align: right;
        }

        .stButton > button {
            border-radius: 16px !important;
            font-size: 1.05rem !important;
            font-weight: 850 !important;
            padding: 0.85rem 1rem !important;
        }

        div[data-testid="stDownloadButton"] > button {
            background: #16a34a !important;
            color: white !important;
            border: 0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_config_section() -> str | None:
    """
    Muestra la ruta oficial de salida.

    La ruta no se edita desde pantalla para evitar que un operador
    cambie accidentalmente el destino de los PDFs.
    """
    ruta_actual = obtener_pdf_output_dir()

    st.markdown("### Configuración de salida de PDFs")

    st.info(
        "La ruta de salida está protegida y no puede modificarse desde la pantalla."
    )

    st.code(str(ruta_actual), language="text")

    validacion = validar_directorio_salida(str(ruta_actual))

    if not validacion["ok"]:
        st.error(validacion["error"])
        return None

    return str(validacion["path"])


def _formatear_texto(valor: object, vacio: str = "-") -> str:
    texto = str(valor or "").strip()
    return texto if texto else vacio


def _formatear_ts_visible(valor: object, vacio: str = "-") -> str:
    texto = str(valor or "").strip()
    if not texto:
        return vacio
    dt = _parse_ts(texto)
    if not dt:
        return texto
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _render_tarjeta_resumen(titulo: str, filas: list[tuple[str, str]], variante: str = "normal") -> None:
    clase = f"op-card op-card-{variante}"
    cuerpo = "".join(
        f"""
        <div class="op-card-row">
            <span>{html.escape(etiqueta)}</span>
            <strong>{html.escape(valor)}</strong>
        </div>
        """
        for etiqueta, valor in filas
    )
    st.markdown(
        f"""
        <div class="{clase}">
            <div class="op-card-title">{html.escape(titulo)}</div>
            {cuerpo}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_cola_pendiente(username: str, password: str, fe_pla_aniomes_desde: str, dig_tramite: str = ""):
    st.markdown("### Próximos 200 trámites en cola")

    col_a, col_b = st.columns([1, 1])
    with col_a:
        refrescar = st.button(
            "Actualizar cola",
            key="btn_actualizar_cola_200",
            use_container_width=True,
        )
    with col_b:
        mostrar = st.checkbox(
            "Mostrar cola pendiente",
            value=True,
            key="chk_mostrar_cola_200",
        )

    if not mostrar:
        st.caption("La cola quedó oculta por el operador.")
        return

    cache_key = f"cola_200::{fe_pla_aniomes_desde}::{(dig_tramite or '').strip()}"
    if refrescar:
        st.session_state.pop(cache_key, None)

    filas = st.session_state.get(cache_key)

    if filas is None:
        with st.spinner("Consultando los próximos trámites pendientes en Oracle..."):
            try:
                filas = obtener_tramites_en_cola(
                    username=username,
                    password=password,
                    fe_pla_aniomes_desde=str(fe_pla_aniomes_desde).strip(),
                    limite=200,
                    dig_tramite=(dig_tramite or "").strip() or None,
                )
                st.session_state[cache_key] = filas
            except Exception as exc:
                st.error("No se pudo consultar la cola pendiente.")
                st.code(str(exc), language="text")
                return

    estado = leer_estado_job() or {}
    siguiente = filas[0] if filas else {}
    col_res_1, col_res_2 = st.columns(2)
    with col_res_1:
        _render_tarjeta_resumen(
            "Última procesada",
            [
                ("Trámite", _formatear_texto(estado.get("last_processed_tramite"))),
                ("Cédula", _formatear_texto(estado.get("last_processed_cedula"))),
                ("Planilla", _formatear_texto(estado.get("last_processed_planilla"))),
                ("Mes", _formatear_texto(estado.get("last_processed_fe_pla_aniomes"))),
                ("Hora", _formatear_ts_visible(estado.get("last_processed_at"))),
                ("Estado", _formatear_texto(estado.get("last_processed_status"))),
            ],
            variante="primary",
        )
    with col_res_2:
        if siguiente:
            _render_tarjeta_resumen(
                "Siguiente en cola",
                [
                    ("Trámite", _formatear_texto(siguiente.get("DIG_TRAMITE"))),
                    ("Cédula", _formatear_texto(siguiente.get("DIG_CEDULA"))),
                    ("Planilla", _formatear_texto(siguiente.get("DIG_TRAMITE"))),
                    ("Mes", _formatear_texto(siguiente.get("FE_PLA_ANIOMES"))),
                    ("Hora", _formatear_texto(siguiente.get("DIG_FECHA_PLANILLA"))),
                    ("ID trámite", _formatear_texto(siguiente.get("DIG_ID_TRAMITE"))),
                ],
                variante="accent",
            )
        else:
            _render_tarjeta_resumen(
                "Siguiente en cola",
                [
                    ("Trámite", "Sin pendientes"),
                    ("Cédula", "-"),
                    ("Planilla", "-"),
                    ("Mes", "-"),
                    ("Hora", "-"),
                    ("ID trámite", "-"),
                ],
                variante="accent",
            )

    total = len(filas)
    st.caption(f"Se muestran {total} trámite(s) pendientes de un máximo de 200.")

    if not filas:
        st.info("No hay trámites pendientes en cola con los filtros actuales.")
        return

    df = pd.DataFrame(filas).copy()

    columnas_preferidas = [
        "DIG_TRAMITE",
        "DIG_CEDULA",
        "FE_PLA_ANIOMES",
        "DIG_ID_TRAMITE",
        "DIG_ID_GENERACION",
        "DIG_PLANILLADO",
        "DIG_COBERTURA",
        "DIG_DEPENDIENTE_01",
        "DIG_DEPENDIENTE_02",
        "DIG_FECHA_PLANILLA",
    ]

    columnas_visibles = [c for c in columnas_preferidas if c in df.columns]
    if columnas_visibles:
        df = df[columnas_visibles]

    df.insert(0, "ORDEN_COLA", range(1, len(df) + 1))

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=420,
    )

    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "Descargar cola CSV",
        data=csv_bytes,
        file_name=f"cola_pendiente_{fe_pla_aniomes_desde}.csv",
        mime="text/csv",
        use_container_width=True,
        key="btn_descargar_cola_csv",
    )


def _ejecutar_sync_coberturas_repo(
    origen_root: str,
    dig_tramite: str = "",
) -> dict:
    """
    Ejecuta la sincronización hacia el repositorio oficial y muestra en Streamlit
    el bloque vivo de los últimos 100 resultados.

    Muestra:
    - Trámite
    - Archivo
    - Estado
    - Destino

    El CSV y SQLite siguen guardando todo el histórico completo.
    """
    project_root = Path("/data_nuevo/cobertura_integrada")
    script_path = project_root / "scripts" / "sync_coberturas_repo.py"

    if not script_path.exists():
        return {
            "ok": False,
            "already_running": False,
            "returncode": -1,
            "stdout": "",
            "stderr": f"No existe el script de sync: {script_path}",
            "manifest_path": "",
        }

    cmd = [
        sys.executable,
        "-u",
        str(script_path),
        "--origen-root",
        str(Path(origen_root).resolve()),
        "--repo-root",
        "/data_nuevo/repo_grande/data/datos",
        "--logs-dir",
        "/data_nuevo/cobertura_integrada/logs",
        "--state-db",
        "/data_nuevo/cobertura_integrada/logs/cobertura_repo_sync.sqlite",
        "--apply",
        "--emit-json-events",
        "--batch-ui-size",
        "100",
    ]

    if dig_tramite:
        cmd.extend(["--tramite", dig_tramite])

    st.markdown("### Sincronización hacia repositorio oficial")

    sync_status = st.empty()
    sync_metrics = st.empty()
    sync_table = st.empty()
    sync_log = st.empty()

    ultimos_100 = deque(maxlen=100)
    contador_estados = Counter()
    stdout_lines: list[str] = []
    manifest_path = ""
    run_id = ""

    def render_tabla():
        if not ultimos_100:
            return

        df = pd.DataFrame(list(ultimos_100))

        sync_table.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
        )

    def render_metricas():
        total_eventos = sum(contador_estados.values())

        col1, col2, col3, col4 = sync_metrics.columns(4)

        col1.metric("Resultados sync", total_eventos)
        col2.metric("Copiados", contador_estados.get("COPIADO", 0))
        col3.metric(
            "Ya existían",
            contador_estados.get("OMITIDO_YA_EXISTE_IDENTICO", 0)
            + contador_estados.get("OMITIDO_YA_EXISTE_DIFERENTE", 0),
        )
        col4.metric(
            "Con observación",
            contador_estados.get("DESTINO_NO_ENCONTRADO", 0)
            + contador_estados.get("DESTINO_AMBIGUO", 0)
            + contador_estados.get("ERROR_COPIA", 0)
            + contador_estados.get("ERROR_HASH", 0)
            + contador_estados.get("SIN_PDFS_CC", 0)
            + contador_estados.get("ORIGEN_NO_EXISTE", 0),
        )

    try:
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        process = subprocess.Popen(
            cmd,
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )

        sync_status.markdown(
            """
            <div class="status-info">
                Sincronización iniciada. Mostrando los últimos 100 resultados...
            </div>
            """,
            unsafe_allow_html=True,
        )

        assert process.stdout is not None

        for raw_line in process.stdout:
            line = raw_line.rstrip("\n")
            stdout_lines.append(line)

            if not line.startswith("SYNC_EVENT_JSON "):
                continue

            try:
                payload = json.loads(line.replace("SYNC_EVENT_JSON ", "", 1))
            except Exception:
                continue

            event_type = payload.get("event_type", "")

            if event_type == "RUN_START":
                run_id = payload.get("run_id", "")
                sync_status.info(f"Sync iniciado. Run ID: {run_id}")

            elif event_type == "INDEX_FINISHED":
                sync_status.info(
                    "Índice destino construido. "
                    f"Trámites origen: {payload.get('tramites_origen', 0)} | "
                    f"Carpetas destino indexadas: {payload.get('carpetas_destino_indexadas', 0)}"
                )

            elif event_type == "TRAMITE_START":
                sync_log.caption(
                    f"Revisando trámite {payload.get('tramite')} "
                    f"({payload.get('index')} de {payload.get('total_tramites')})"
                )

            elif event_type == "FILE_RESULT":
                estado = payload.get("estado", "")
                contador_estados[estado] += 1

                ultimos_100.append(
                    {
                        "N°": payload.get("sequence", ""),
                        "Trámite": payload.get("tramite", ""),
                        "Archivo": payload.get("archivo", ""),
                        "Estado": estado,
                        "Destino": payload.get("destino", ""),
                        "Detalle": payload.get("detalle", ""),
                    }
                )

                sequence = int(payload.get("sequence") or 0)

                if sequence == 1 or sequence % 10 == 0:
                    render_metricas()
                    render_tabla()

            elif event_type == "BATCH_MARK":
                render_metricas()
                render_tabla()
                sync_status.info(
                    f"Bloque procesado: {payload.get('sequence')} resultados revisados."
                )

            elif event_type == "RUN_END":
                manifest_path = payload.get("manifest_path", "")
                render_metricas()
                render_tabla()
                sync_status.success(
                    f"Sync terminado. CSV: {manifest_path}"
                )

        returncode = process.wait()

        # Render final por si no cayó exactamente en múltiplo de 10.
        render_metricas()
        render_tabla()

        stdout = "\n".join(stdout_lines)

        if not manifest_path:
            for line in stdout_lines:
                if line.startswith("CSV detalle:"):
                    manifest_path = line.replace("CSV detalle:", "").strip()

        return {
            "ok": returncode == 0,
            "already_running": returncode == 10,
            "returncode": returncode,
            "stdout": stdout,
            "stderr": "",
            "manifest_path": manifest_path,
            "run_id": run_id,
            "estados": dict(contador_estados),
        }

    except Exception as exc:
        return {
            "ok": False,
            "already_running": False,
            "returncode": -3,
            "stdout": "\n".join(stdout_lines),
            "stderr": str(exc),
            "manifest_path": manifest_path,
            "run_id": run_id,
            "estados": dict(contador_estados),
        }


def _render_estado_worker():
    """Lee el estado persistente del worker y lo muestra como panel de monitor en vivo."""
    estado = leer_estado_job() or {}

    st.markdown("---")
    st.markdown("### 📡 Estado automático en vivo")

    if not estado or not estado.get("enabled"):
        st.info("El worker autónomo no está activo. Presione 'Generar coberturas automáticas' para activarlo.")
        return

    # Fila 1: estado general
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Estado", str(estado.get("status", "-")))
    with col2:
        st.metric("Mes desde", str(estado.get("fe_pla_aniomes_desde", "-")))
    with col3:
        st.metric("Pendientes antes", str(estado.get("pendientes_antes", "-")))
    with col4:
        st.metric("Pendientes después", str(estado.get("pendientes_despues", "-")))

    # Fila 2: métricas de última pasada
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.metric("Generados", str(estado.get("last_generados", "-")))
    with col6:
        st.metric("Actualizados", str(estado.get("last_actualizados", "-")))
    with col7:
        st.metric("Errores", str(estado.get("last_errores", "-")))
    with col8:
        st.metric("PID worker", str(estado.get("launcher_pid", "-")))

    detalle = str(estado.get("detalle", "") or "").strip()
    last_error = str(estado.get("last_error", "") or "").strip()
    updated_at = str(estado.get("updated_at", "") or "").strip()

    if detalle:
        st.info(detalle)
    if updated_at:
        st.caption(f"Última actualización: {updated_at}")

    if last_error:
        st.warning(last_error)


def _leer_json_seguro(path: Path) -> dict:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


@st.fragment(run_every="5s")
def _render_monitor_progreso():
    """Muestra el estado real del worker y del watchdog en una sola tarjeta."""
    state_path = Path("/data_nuevo/cobertura_integrada/logs/cobertura_auto_resume_state.json")
    watchdog_state_path = Path("/data_nuevo/cobertura_integrada/logs/cobertura_watchdog_state.json")
    watchdog_log_path = Path("/data_nuevo/cobertura_integrada/logs/cobertura_watchdog.log")

    worker = _leer_json_seguro(state_path)
    watchdog = _leer_json_seguro(watchdog_state_path)

    st.markdown("### 📈 Avance real")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Estado worker", str(worker.get("status", "-")))
    with col2:
        st.metric("Pendientes antes", str(worker.get("pendientes_antes", "-")))
    with col3:
        st.metric("Pendientes después", str(worker.get("pendientes_despues", "-")))
    with col4:
        st.metric("Sync activo", "Sí" if worker.get("sync_active") else "No")

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.metric("Generados", str(worker.get("last_generados", "-")))
    with col6:
        st.metric("Actualizados", str(worker.get("last_actualizados", "-")))
    with col7:
        st.metric("Errores", str(worker.get("last_errores", "-")))
    with col8:
        st.metric("Último trámite", str(worker.get("sync_active_tramite") or worker.get("dig_tramite", "-") or "-"))

    detalle = str(worker.get("detalle", "") or "").strip()
    last_progress_at = str(worker.get("last_progress_at", "") or "").strip()
    sync_since = str(worker.get("sync_active_since", "") or "").strip()
    updated_at = str(worker.get("updated_at", "") or "").strip()
    seconds_update = _segundos_desde(updated_at)
    seconds_progress = _segundos_desde(last_progress_at)
    semaforo_estado, semaforo_texto = _estado_semaforo(
        seconds_progress,
        bool(worker.get("sync_active")),
        str(worker.get("status", "") or ""),
    )

    st.markdown(
        f"""
        <div class="status-card {semaforo_estado}">
            <strong>Estado visual:</strong> {semaforo_texto}
        </div>
        """,
        unsafe_allow_html=True,
    )

    if detalle:
        st.info(detalle)

    info_cols = st.columns(3)
    with info_cols[0]:
        st.caption(f"Último progreso: {last_progress_at or '-'}")
    with info_cols[1]:
        st.caption(f"Sync desde: {sync_since or '-'}")
    with info_cols[2]:
        st.caption(f"Actualizado: {updated_at or '-'}")

    age_cols = st.columns(2)
    with age_cols[0]:
        st.caption(f"Segundos sin progreso: {'-' if seconds_progress is None else seconds_progress}")
    with age_cols[1]:
        st.caption(f"Segundos desde actualización: {'-' if seconds_update is None else seconds_update}")

    if watchdog:
        verdict = watchdog.get("verdict", {}) or {}
        wcols = st.columns(4)
        with wcols[0]:
            st.metric("Watchdog", str(verdict.get("action", "-")))
        with wcols[1]:
            st.metric("Servicio", str(watchdog.get("service_active", "-")))
        with wcols[2]:
            st.metric("PID worker", str(watchdog.get("main_pid", "-")))
        with wcols[3]:
            st.metric("PID sync", str(watchdog.get("sync_pid", "-")))

        verdict_reason = str(verdict.get("reason", "") or "").strip()
        if verdict_reason:
            st.caption(verdict_reason)

        checked_at = str(watchdog.get("checked_at", "") or "").strip()
        if checked_at:
            st.caption(f"Último chequeo watchdog: {checked_at}")

    if watchdog_log_path.exists():
        try:
            last_line = watchdog_log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-1]
            st.caption(f"Watchdog log: {last_line}")
        except Exception:
            pass


def _render_auto_result():
    """Muestra resultados de ejecución directa (legado, mantenido para compatibilidad)."""
    result = st.session_state.get("current_result")

    if not result:
        return

    total = result.get("total", 0)
    generados = result.get("generados", 0)
    actualizados = result.get("actualizados", 0)
    errores = result.get("errores", 0)

    st.markdown(
        f"""
        <div class="status-success">
            Proceso finalizado<br>
            Total encontrados: {total} |
            Generados correctamente: {generados} |
            Actualizados en Oracle: {actualizados} |
            Errores: {errores}
        </div>
        """,
        unsafe_allow_html=True,
    )

    manifest_path_value = result.get("manifest_path")

    if manifest_path_value:
        manifest_path = Path(manifest_path_value)

        if manifest_path.exists():
            with manifest_path.open("rb") as file:
                st.download_button(
                    "Descargar manifiesto CSV",
                    data=file,
                    file_name=manifest_path.name,
                    mime="text/csv",
                    use_container_width=True,
                )

    for label, key_name in [
        ("Descargar log t\u00e9cnico JSONL", "run_log_path"),
        ("Descargar log de errores JSONL", "error_log_path"),
    ]:
        value = result.get(key_name)
        if value:
            log_path = Path(value)
            if log_path.exists():
                with log_path.open("rb") as file:
                    st.download_button(
                        label,
                        data=file,
                        file_name=log_path.name,
                        mime="application/json",
                        use_container_width=True,
                    )

    run_log_path = result.get("run_log_path")
    if run_log_path and Path(run_log_path).exists():
        with st.expander("Ver \u00faltimos eventos t\u00e9cnicos"):
            lines = Path(run_log_path).read_text(
                encoding="utf-8",
                errors="replace",
            ).splitlines()[-30:]
            for line in lines:
                st.code(line, language="json")

    sync_repo = result.get("sync_repo")

    if sync_repo:
        st.markdown("### Resultado de sincronización al repositorio")

        if sync_repo.get("ok"):
            st.success("Sincronización finalizada correctamente.")
        elif sync_repo.get("already_running"):
            st.info("La sincronización no se ejecutó porque ya había otra en curso.")
        else:
            st.warning("La sincronización terminó con observaciones.")

        estados = sync_repo.get("estados") or {}

        if estados:
            st.markdown("#### Estados del sync")
            st.dataframe(
                pd.DataFrame(
                    [{"Estado": estado, "Total": total} for estado, total in estados.items()]
                ),
                use_container_width=True,
                hide_index=True,
            )

        manifest_sync = sync_repo.get("manifest_path")

        if manifest_sync and Path(manifest_sync).exists():
            with Path(manifest_sync).open("rb") as file:
                st.download_button(
                    "Descargar CSV completo del sync",
                    data=file,
                    file_name=Path(manifest_sync).name,
                    mime="text/csv",
                    use_container_width=True,
                )

        with st.expander("Ver salida técnica del sync"):
            if sync_repo.get("stdout"):
                st.code(sync_repo.get("stdout"), language="text")
            if sync_repo.get("stderr"):
                st.code(sync_repo.get("stderr"), language="text")

    errors = result.get("errors") or []

    if errors:
        st.markdown(
            f"""
            <div class="status-warn">
                Se detectaron {len(errors)} registros con problemas.
                Revise el detalle antes de cerrar el proceso.
            </div>
            """,
            unsafe_allow_html=True,
        )

        with st.expander(f"Ver diagnóstico de {len(errors)} errores"):
            for item in errors:
                st.markdown(
                    f"""
                    **Trámite:** `{item.get('dig_tramite')}`  
                    **ID trámite:** `{item.get('dig_id_tramite')}`  
                    **Cédula:** `{item.get('cedula')}`  
                    **PDF esperado:** `{item.get('pdf_esperado', '')}`  
                    **Categoría:** `{item.get('categoria', '')}`  
                    **Causa probable:** {item.get('causa', item.get('error', ''))}  
                    **Qué revisar:** {item.get('sugerencia', '')}
                    """
                )

                with st.expander("Ver detalle técnico de este error"):
                    st.code(item.get("error", ""))


def _obtener_mes_desde_por_defecto() -> str:
    """
    Mantiene compatibilidad con .env.
    Si no existe AUTO_FE_PLA_ANIOMES_DESDE, usa 202604.
    """
    mes = os.getenv("AUTO_FE_PLA_ANIOMES_DESDE", "202604").strip()

    if not mes:
        return "202604"

    return mes


def _validar_fe_pla_aniomes_desde(valor: str) -> tuple[bool, str, str]:
    """
    Valida el mes desde para evitar consultas accidentales o valores mal escritos.
    Formato permitido: YYYYMM. Ejemplo: 202604.
    """
    mes = str(valor or "").strip()

    if not re.fullmatch(r"\d{6}", mes):
        return False, mes, "El mes desde debe tener formato YYYYMM. Ejemplo válido: 202604."

    numero_mes = int(mes[4:6])

    if numero_mes < 1 or numero_mes > 12:
        return False, mes, "El mes debe estar entre 01 y 12. Ejemplo válido: 202604."

    return True, mes, ""


def _render_operator_panel():
    st.markdown("---")
    st.markdown("### 🛠️ Panel de operación segura")

    estado = leer_estado_operador()

    cuarentena_count = int(estado.get("quarantine_count", 0))
    stop_flag = bool(estado.get("stop_flag"))
    gen_lock = estado.get("generation_lock", {})
    sync_lock = estado.get("sync_lock", {})
    job = estado.get("job", {})

    lock_activo = bool(gen_lock.get("held") or sync_lock.get("held"))
    lock_huerfano = bool(gen_lock.get("orphan") or sync_lock.get("orphan"))

    col_a, col_b, col_c, col_d = st.columns(4)

    with col_a:
        st.metric("Cuarentena", cuarentena_count)
    with col_b:
        st.metric("Parada", "Activa" if stop_flag else "Inactiva")
    with col_c:
        if lock_activo:
            st.metric("Proceso", "Trabajando")
        elif lock_huerfano:
            st.metric("Proceso", "Bloq. huérfano")
        else:
            st.metric("Proceso", "Libre")
    with col_d:
        st.metric("Reintento", str(job.get("status", "Sin estado")))

    if lock_activo:
        st.info(
            "Hay un proceso activo. No se debe destrabar todavía. "
            "Si los PDFs siguen apareciendo, el sistema está trabajando."
        )
    elif cuarentena_count > 0:
        st.warning(
            "Hay trámites en cuarentena temporal. Si el proceso no avanza, "
            "podés liberar la cuarentena y permitir un nuevo intento."
        )
    elif lock_huerfano:
        st.warning("Existe un bloqueo huérfano. Esto puede ocurrir tras un corte inesperado.")
    elif stop_flag:
        st.warning("La bandera de parada está activa. El proceso no tomará nuevos registros.")
    else:
        st.success("El estado operativo no muestra bloqueos críticos.")

    with st.expander("Ver trámites en cuarentena", expanded=cuarentena_count > 0):
        rows = estado.get("quarantine_rows", [])
        if rows:
            df = pd.DataFrame(rows)
            columnas = ["tramite", "minutos_restantes", "retry_count", "motivo", "created_at_local", "expires_at_local", "clave"]
            columnas_existentes = [c for c in columnas if c in df.columns]
            st.dataframe(df[columnas_existentes], use_container_width=True, hide_index=True)
        else:
            st.caption("No hay trámites en cuarentena activa.")

    with st.expander("Ver estado interno", expanded=False):
        st.json(job)

    with st.expander("Ver últimos eventos técnicos", expanded=False):
        errores = estado.get("last_errors", [])
        if errores:
            st.code("\n".join(errores[-40:]), language="text")
        else:
            st.caption("No hay errores recientes relevantes.")

    st.markdown("#### Acciones seguras")
    st.caption(
        "Estas acciones no borran PDFs, no modifican Oracle y no matan procesos activos. "
        "Solo liberan bloqueos temporales de la aplicación."
    )

    confirmar = st.checkbox(
        "Confirmo que el proceso no está avanzando y deseo liberar bloqueos temporales",
        key="confirmar_destrabe_seguro",
    )

    col1, col2 = st.columns([2, 1])

    with col1:
        if st.button(
            "Liberar cuarentena y permitir reintento",
            key="btn_destrabar_reintento_seguro",
            use_container_width=True,
            disabled=not confirmar or lock_activo,
        ):
            resultado = destrabar_para_reintento()
            if resultado.get("ok"):
                st.success("Listo. Se liberó cuarentena, se limpió parada y se habilitó reintento.")
            else:
                st.warning("No se habilitó reintento porque hay un proceso activo.")
            st.json(resultado)
            st.rerun()

    with col2:
        if st.button(
            "Pausar reintento automático",
            key="btn_pausar_reintento_auto",
            use_container_width=True,
        ):
            pausar_reintento_automatico()
            st.warning("Reintento automático pausado.")
            st.rerun()

    with st.expander("Copiar diagnóstico para soporte", expanded=False):
        st.code(exportar_estado_json(), language="json")


def dashboard_page():
    _init_state()
    _render_css()

    mes_por_defecto = _obtener_mes_desde_por_defecto()

    st.markdown(
        """
        <div class="main-title">Cobertura automática MSP</div>
        <div class="main-subtitle">
            Monitor operativo. El worker corre solo por timer cada 2 min.
            Usar contingencia solo si el sistema no responde.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="simple-card">', unsafe_allow_html=True)

    # Sección de configuración de ruta
    pdf_output_dir = _render_config_section()
    ruta_valida = pdf_output_dir is not None

    st.markdown("---")

    # Filtro de procesamiento
    st.markdown("### Filtro de procesamiento")

    modo_procesamiento = st.radio(
        "Seleccione cómo desea procesar",
        options=[
            "Procesar por mes desde",
            "Procesar por trámite específico",
        ],
        horizontal=True,
        key="modo_procesamiento_cobertura",
    )

    dig_tramite_input = ""
    tramite_valido = True

    if modo_procesamiento == "Procesar por trámite específico":
        dig_tramite_input = st.text_input(
            "Número de trámite",
            value="",
            max_chars=30,
            key="dig_tramite_input",
            help="Ejemplo: 5899568. Solo se procesará ese DIG_TRAMITE.",
        ).strip()

        if dig_tramite_input and not dig_tramite_input.isdigit():
            st.error("El trámite debe contener solo números.")
            tramite_valido = False
        elif not dig_tramite_input:
            st.warning("Ingrese un número de trámite para continuar.")
            tramite_valido = False
        else:
            st.info(f"Se procesará únicamente el trámite {dig_tramite_input}.")
            tramite_valido = True

        if dig_tramite_input and dig_tramite_input.isdigit():
            st.markdown("#### Acciones rápidas")
            if st.button(
                "📦 Enviar trámite a cola de resincronización",
                key="btn_cola_sync",
                use_container_width=True,
                help="NO genera PDF. NO toca Oracle. Solo intenta sincronizar PDFs existentes al repositorio.",
            ):
                from src.auto_resume_state import marcar_tramite_sync_pendiente
                marcar_tramite_sync_pendiente(
                    tramite=dig_tramite_input,
                    source_dir=f"/data_nuevo/coberturas/{dig_tramite_input}",
                )
                st.success(f"Trámite {dig_tramite_input} enviado a cola de resincronización.")
                st.info("El worker lo procesará en la siguiente pasada.")
    else:
        tramite_valido = True

    st.markdown("---")

    mes_key = f"fe_pla_aniomes_desde_input_{st.session_state.input_reset_counter}"

    fe_pla_aniomes_input = st.text_input(
        "Mes desde",
        value=mes_por_defecto,
        max_chars=6,
        key=mes_key,
        help="Formato YYYYMM. Ejemplo: 202604 procesa FE_PLA_ANIOMES >= 202604.",
    )

    mes_valido, fe_pla_aniomes_desde, error_mes = _validar_fe_pla_aniomes_desde(
        fe_pla_aniomes_input
    )

    if mes_valido:
        st.caption(
            f"Se procesarán registros con FE_PLA_ANIOMES >= {fe_pla_aniomes_desde}."
        )
    else:
        st.error(error_mes)

    # Contador de pendientes
    pendientes_oracle_actuales = st.session_state.get("pendientes_inicio_corrida")
    proceso_activo = bool(st.session_state.get("proceso_en_curso", False))
    username = st.session_state.get("oracle_user", "")
    password = st.session_state.get("oracle_password", "")

    if mes_valido and tramite_valido:
        contador_tramite = dig_tramite_input if modo_procesamiento == "Procesar por trámite específico" else ""

        if not proceso_activo:
            if st.button("Actualizar contador", key="btn_actualizar_contador", use_container_width=True):
                if username and password:
                    try:
                        with st.spinner("Consultando Oracle..."):
                            pendientes_oracle_actuales = _contar_pendientes_automaticos(
                                username=username, password=password,
                                fe_pla_aniomes_desde=fe_pla_aniomes_desde,
                                dig_tramite=contador_tramite, timeout_seconds=30)
                        st.session_state["pendientes_inicio_corrida"] = int(pendientes_oracle_actuales)
                    except Exception as exc:
                        st.warning(f"No se pudo consultar: {exc}")

        if pendientes_oracle_actuales is not None:
            procesados = int(st.session_state.get("procesados_corrida", 0))
            faltan = max(int(pendientes_oracle_actuales) - procesados, 0)
            c1, c2, c3 = st.columns(3)
            with c1: st.metric("Pendientes en Oracle", f"{int(pendientes_oracle_actuales):,}")
            with c2: st.metric("Procesados en esta corrida", f"{procesados:,}")
            with c3: st.metric("Faltan en esta corrida", f"{faltan:,}")
            if int(pendientes_oracle_actuales) == 0 and not proceso_activo:
                st.info("No hay registros pendientes con los filtros seleccionados.")

    st.markdown("---")

    if mes_valido and tramite_valido:
        _render_cola_pendiente(
            username=username,
            password=password,
            fe_pla_aniomes_desde=fe_pla_aniomes_desde,
            dig_tramite=dig_tramite_input if modo_procesamiento == "Procesar por trámite específico" else "",
        )

    st.markdown("---")

    # Monitor del worker autónomo
    _render_estado_worker()

    st.markdown("---")

    puede_generar = ruta_valida and mes_valido and tramite_valido

    with st.expander("⚙️ Contingencia: forzar corrida manual", expanded=False):
        st.caption("El sistema corre solo por timer. Usar solo si el worker no responde o para trámite puntual urgente.")
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            generar = st.button(
                "Forzar corrida manual",
                key="generar_auto_button",
                use_container_width=True,
                disabled=not puede_generar,
            )
        with col2:
            parar = st.button(
                "Parar proceso",
                key="parar_auto_button",
                use_container_width=True,
            )
        with col3:
            limpiar = st.button(
                "Limpiar",
                key="limpiar_auto_button",
                use_container_width=True,
            )

    if parar:
        _crear_bandera_parada()
        st.warning("Se solicitó detener el proceso. Terminará la fila actual y no tomará una nueva.")

    if limpiar:
        _limpiar_bandera_parada()
        _reset_all()
        st.rerun()

    if generar:
        _limpiar_bandera_parada()
        registrar_job_activo(
            fe_pla_aniomes_desde=fe_pla_aniomes_desde,
            output_dir=str(pdf_output_dir),
            dig_tramite=dig_tramite_input,
        )

        lanzamiento = _disparar_worker_inmediato()

        if lanzamiento.get("ok"):
            guardar_estado_job({
                "launcher_pid": lanzamiento.get("pid"),
                "detalle": (
                    f"Trabajo armado desde Streamlit. "
                    f"Worker inmediato lanzado PID={lanzamiento.get('pid')}."
                ),
                "last_error": "",
            })
            st.success("Modo autónomo activado.")
            st.info("El worker seguirá vigilando Oracle y procesará nuevos trámites sin más clics.")
            st.session_state["worker_recien_lanzado"] = True
        else:
            marcar_job_reintento(lanzamiento.get("error", "No se pudo lanzar el worker."))
            st.error("No se pudo lanzar el worker inmediato.")
            st.code(str(lanzamiento.get("error", "")), language="text")

        st.rerun()

    _render_monitor_progreso()
    _render_auto_result()
    _render_operator_panel()

    st.markdown("</div>", unsafe_allow_html=True)

    st.write("")

    if st.button(
        "Salir",
        key="minimal_logout_button",
        use_container_width=True,
    ):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()
