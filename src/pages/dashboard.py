from pathlib import Path
import time

import streamlit as st

from src.async_jobs import submit_job, render_current_job
from src.cobertura_pdf import (
    contar_registros_cobertura,
    generar_hojas_cobertura_por_id,
)


DEFAULT_SEARCH_TIMEOUT_SECONDS = 60
DEFAULT_GENERATE_TIMEOUT_SECONDS = 3600
AUTO_REFRESH_SECONDS = 2


def _clear_job_state():
    for key in [
        "current_future",
        "current_job_name",
        "current_result",
        "current_error",
        "current_timeout_seconds",
        "current_started_at_utc",
        "current_timed_out_ui",
    ]:
        if key in st.session_state:
            del st.session_state[key]


def _clear_search_state():
    for key in [
        "codigo_buscado",
        "codigo_encontrado",
        "codigo_total_registros",
        "codigo_validado",
    ]:
        if key in st.session_state:
            del st.session_state[key]


def _reset_all():
    _clear_job_state()
    _clear_search_state()
    st.session_state.input_reset_counter += 1


def _init_state():
    defaults = {
        "codigo_buscado": "",
        "codigo_encontrado": False,
        "codigo_total_registros": 0,
        "codigo_validado": False,
        "input_reset_counter": 0,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _render_css():
    st.markdown(
        """
        <style>
        .block-container {
            max-width: 760px;
            padding-top: 2rem;
        }

        section[data-testid="stSidebar"] {
            display: none;
        }

        .main-title {
            text-align: center;
            font-size: 2rem;
            font-weight: 850;
            color: #0f172a;
            margin-bottom: 0.35rem;
        }

        .main-subtitle {
            text-align: center;
            color: #64748b;
            font-size: 1rem;
            margin-bottom: 2rem;
        }

        .simple-card {
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 24px;
            padding: 1.6rem;
            box-shadow: 0 14px 38px rgba(15, 23, 42, 0.08);
            margin-bottom: 1.2rem;
        }

        .status-ok {
            background: #ecfdf5;
            border: 1px solid #bbf7d0;
            color: #166534;
            border-radius: 18px;
            padding: 1rem;
            font-weight: 800;
            text-align: center;
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

        .stTextInput input {
            font-size: 1.15rem !important;
            border-radius: 16px !important;
            padding: 0.85rem !important;
        }

        .stButton > button {
            border-radius: 16px !important;
            font-size: 1.05rem !important;
            font-weight: 850 !important;
            padding: 0.85rem 1rem !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _get_ready_result() -> dict | None:
    result = st.session_state.get("current_result")

    if not result:
        return None

    if not isinstance(result, dict):
        return None

    if not result.get("ok"):
        return None

    return result


def _render_result():
    result = _get_ready_result()

    if not result:
        return

    st.markdown(
        f"""
        <div class="status-success">
            Proceso finalizado<br>
            Total: {result.get("total", 0)} |
            Generados: {result.get("generated", 0)} |
            Omitidos: {result.get("skipped", 0)} |
            Errores: {result.get("failed", 0)}
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.info(f"Carpeta principal: {result.get('output_root')}")

    manifest_path = Path(result.get("manifest_path", ""))

    if manifest_path.exists():
        with manifest_path.open("rb") as file:
            st.download_button(
                "Descargar manifiesto CSV",
                data=file,
                file_name=manifest_path.name,
                mime="text/csv",
                use_container_width=True,
            )

    errors = result.get("errors") or []

    if errors:
        with st.expander("Ver errores"):
            for item in errors:
                st.write(
                    f"Planilla: `{item.get('planilla')}` | "
                    f"Cédula: `{item.get('cedula')}` | "
                    f"Fecha: `{item.get('fecha')}`"
                )
                st.code(item.get("error", ""))


def _auto_refresh_if_running():
    future = st.session_state.get("current_future")

    if future and future.running():
        time.sleep(AUTO_REFRESH_SECONDS)
        st.rerun()


def dashboard_page():
    _init_state()
    _render_css()

    st.markdown(
        """
        <div class="main-title">Generar hojas de cobertura</div>
        <div class="main-subtitle">
            Ingrese el código de generación. El sistema creará una carpeta por planilla
            y guardará dentro la hoja de cobertura de cada cédula.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="simple-card">', unsafe_allow_html=True)

    future = st.session_state.get("current_future")
    proceso_en_ejecucion = bool(future and future.running())
    resultado_listo = _get_ready_result() is not None

    input_key = f"cobertura_id_generacion_input_{st.session_state.input_reset_counter}"

    codigo_generacion = st.text_input(
        "Código de generación",
        placeholder="Ejemplo: 12345",
        key=input_key,
        disabled=proceso_en_ejecucion,
    )

    overwrite = st.checkbox(
        "Sobrescribir PDFs si ya existen",
        value=False,
        disabled=proceso_en_ejecucion,
    )

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        buscar = st.button(
            "Buscar",
            key="buscar_coberturas_button",
            use_container_width=True,
            disabled=proceso_en_ejecucion,
        )

    with col2:
        generar = st.button(
            "Generar PDFs",
            key="generar_coberturas_button",
            use_container_width=True,
            disabled=proceso_en_ejecucion or resultado_listo,
        )

    with col3:
        limpiar = st.button(
            "Limpiar",
            key="limpiar_coberturas_button",
            use_container_width=True,
        )

    if limpiar:
        _reset_all()
        st.rerun()

    if buscar:
        _clear_job_state()
        _clear_search_state()

        codigo_limpio = codigo_generacion.strip()

        if not codigo_limpio:
            st.warning("Ingrese el código de generación.")
        else:
            with st.spinner("Buscando registros en Oracle..."):
                try:
                    search_result = contar_registros_cobertura(
                        st.session_state.oracle_user,
                        st.session_state.oracle_password,
                        codigo_limpio,
                        DEFAULT_SEARCH_TIMEOUT_SECONDS,
                    )

                    total = int(search_result["rows"])
                    encontrado = bool(search_result["found"])

                    st.session_state.codigo_buscado = codigo_limpio
                    st.session_state.codigo_validado = True
                    st.session_state.codigo_encontrado = encontrado
                    st.session_state.codigo_total_registros = total

                    st.rerun()

                except Exception as exc:
                    st.session_state.codigo_validado = False
                    st.session_state.codigo_encontrado = False
                    st.session_state.codigo_total_registros = 0
                    st.error("No se pudo realizar la búsqueda.")
                    st.code(str(exc))

    if generar:
        _clear_job_state()

        codigo_limpio = codigo_generacion.strip()

        if not codigo_limpio:
            st.warning("Ingrese el código de generación.")
        else:
            submit_job(
                "Generando hojas de cobertura",
                generar_hojas_cobertura_por_id,
                st.session_state.oracle_user,
                st.session_state.oracle_password,
                codigo_limpio,
                overwrite,
                timeout_seconds=DEFAULT_GENERATE_TIMEOUT_SECONDS,
            )
            st.rerun()

    if st.session_state.codigo_validado:
        if st.session_state.codigo_encontrado:
            st.markdown(
                f"""
                <div class="status-ok">
                    Código encontrado<br>
                    Registros disponibles: {st.session_state.codigo_total_registros}
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                """
                <div class="status-warn">
                    No se encontraron registros para ese código.
                </div>
                """,
                unsafe_allow_html=True,
            )

    if future:
        render_current_job()

    current_error = st.session_state.get("current_error")

    if current_error:
        st.error("No se pudo generar la cobertura.")
        with st.expander("Ver detalle técnico"):
            st.code(current_error)

    _render_result()

    st.markdown("</div>", unsafe_allow_html=True)

    st.write("")

    if st.button(
        "Salir",
        key="minimal_logout_button",
        use_container_width=True,
    ):
        st.session_state.auth_ok = False
        st.session_state.oracle_user = None
        st.session_state.oracle_password = None
        st.session_state.db_user = None
        _reset_all()
        st.rerun()

    _auto_refresh_if_running()
