import json
import os
import re
import subprocess
import sys
from collections import Counter, deque
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
    generar_coberturas_automaticas_desde_mes,
)
from src.cobertura_runner import ejecutar_coberturas_con_lock
from src.auto_resume_state import (
    registrar_job_activo,
    marcar_job_completado,
    marcar_job_reintento,
    marcar_job_detenido_por_usuario,
)


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


def _init_state():
    if "input_reset_counter" not in st.session_state:
        st.session_state.input_reset_counter = 0

    if "current_result" not in st.session_state:
        st.session_state.current_result = None

    if "current_error" not in st.session_state:
        st.session_state.current_error = None


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
    Renderiza la sección de configuración de directorio de salida.
    Devuelve la ruta válida actual o None si hay error.
    """
    config = leer_config()
    ruta_actual = config.get("pdf_output_dir", "/home/crrb/coberturas_generadas/")

    st.markdown("### Configuración de salida de PDFs")

    nueva_ruta = st.text_input(
        "Directorio de salida",
        value=ruta_actual,
        key="pdf_output_dir_input",
    )

    guardar = st.button(
        "Guardar ruta",
        key="guardar_ruta_button",
        use_container_width=True,
    )

    if guardar:
        resultado = validar_directorio_salida(nueva_ruta)

        if resultado["ok"]:
            guardar_config({"pdf_output_dir": str(resultado["path"])})
            st.success(f"Ruta guardada: {resultado['path']}")
            return str(resultado["path"])
        else:
            st.error(resultado["error"])
            return None

    validacion = validar_directorio_salida(nueva_ruta)

    if not validacion["ok"]:
        st.warning(validacion["error"])
        st.caption("Corrija la ruta antes de generar coberturas.")
        return None

    return str(validacion["path"])


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


def _render_auto_result():
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


def dashboard_page():
    _init_state()
    _render_css()

    mes_por_defecto = _obtener_mes_desde_por_defecto()

    st.markdown(
        """
        <div class="main-title">Cobertura automática MSP</div>
        <div class="main-subtitle">
            Genera coberturas desde el mes seleccionado en pantalla,
            solo registros con DIG_COBERTURA='N' y DIG_PLANILLADO='S'.
            Actualiza DIG_COBERTURA='S' solo si el PDF existe.
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

    st.markdown("---")

    puede_generar = ruta_valida and mes_valido and tramite_valido

    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        generar = st.button(
            "Generar coberturas automáticas",
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

    progress_bar = st.empty()
    status_box = st.empty()
    detail_box = st.empty()

    if generar and pdf_output_dir and tramite_valido and mes_valido:
        st.session_state.current_result = None
        st.session_state.current_error = None

        progress_widget = progress_bar.progress(0)

        status_box.markdown(
            """
            <div class="status-info">
                Iniciando proceso...
            </div>
            """,
            unsafe_allow_html=True,
        )

        def on_progress(done: int, total: int, item: dict[str, str]):
            percent = int((done / total) * 100) if total else 0
            progress_widget.progress(percent)

            estado = item.get("estado", "")

            if estado == "GENERADO_Y_ACTUALIZADO":
                emoji = "✅"
            elif "Esperando" in estado:
                emoji = "⏳"
            else:
                emoji = "⚙️"

            procesados_global = item.get("procesados_global", "")
            lote_numero = item.get("lote_numero", "")

            if procesados_global:
                texto_progreso = f"Procesados en esta corrida: {procesados_global}"
            else:
                texto_progreso = f"Procesando lote actual: {done} de {total}"

            if lote_numero:
                texto_progreso += f"<br>Lote consultado: {lote_numero}"

            status_box.markdown(
                f"""
                <div class="status-info">
                    {emoji} {texto_progreso}<br>
                    Avance del lote actual: {percent}%
                </div>
                """,
                unsafe_allow_html=True,
            )

            detail_box.info(
                f"Modo: {modo_procesamiento} | "
                f"Mes desde: {fe_pla_aniomes_desde} | "
                f"Mes registro: {item.get('fe_pla_aniomes')} | "
                f"Trámite: {item.get('dig_tramite')} | "
                f"Cédula: {item.get('dig_cedula')} | "
                f"Estado: {estado}"
            )

        try:
            result = ejecutar_coberturas_con_lock(
                username=st.session_state.oracle_user,
                password=st.session_state.oracle_password,
                fe_pla_aniomes_desde=fe_pla_aniomes_desde,
                dig_tramite=dig_tramite_input,
                output_dir=str(pdf_output_dir),
                progress_callback=on_progress,
            )

            if result.get("generados", 0) > 0:
                status_box.markdown(
                    """
                    <div class="status-info">
                        Coberturas generadas. Sincronizando PDFs hacia el repositorio oficial...
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                sync_result = _ejecutar_sync_coberturas_repo(
                    origen_root=str(pdf_output_dir),
                    dig_tramite=dig_tramite_input if dig_tramite_input else "",
                )

                result["sync_repo"] = sync_result

                if sync_result.get("ok"):
                    st.success("Sincronización al repositorio oficial finalizada.")
                    marcar_job_completado("Proceso terminado desde Streamlit.")
                elif sync_result.get("already_running"):
                    st.info(
                        "Ya existía una sincronización en ejecución. No se inició otra para evitar duplicidad."
                    )
                else:
                    st.warning(
                        "La generación terminó, pero la sincronización al repositorio oficial tuvo observaciones."
                    )
                    if sync_result.get("stderr"):
                        st.code(sync_result.get("stderr", ""), language="text")
                    if sync_result.get("stdout"):
                        st.code(sync_result.get("stdout", ""), language="text")
            else:
                result["sync_repo"] = {
                    "ok": True,
                    "returncode": 0,
                    "stdout": "",
                    "stderr": "",
                    "manifest_path": "",
                    "mensaje": "No se ejecutó sync porque no se generaron PDFs nuevos.",
                }

            progress_widget.progress(100)

            if result.get("total", 0) <= 0 or result.get("sin_pendientes"):
                status_box.markdown(
                    """
                    <div class="status-warn">
                        El proceso terminó sin procesar registros.
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                st.warning(
                    "La aplicación no procesó registros. Si Oracle muestra pendientes, revisar ORACLE_TARGETS, "
                    "la carpeta real desde donde corre Streamlit y los logs."
                )
            else:
                status_box.markdown(
                    """
                    <div class="status-success">
                        Proceso terminado. Manifiesto listo para descargar.
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            detail_box.empty()
            st.session_state.current_result = result

        except Exception as exc:
            st.session_state.current_error = str(exc)
            status_box.markdown(
                """
                <div class="status-warn">
                    Error durante el proceso.
                </div>
                """,
                unsafe_allow_html=True,
            )
            marcar_job_reintento(str(exc))
            st.error("No se pudo completar el proceso automático.")
            st.code(str(exc))

    _render_auto_result()

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
