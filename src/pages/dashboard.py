from pathlib import Path

import streamlit as st

from src.cobertura_pdf import (
    generar_hojas_cobertura_por_id,
    generar_coberturas_automaticas_desde_mes,
)
from src.config import get_jdbc_jar, get_oracle_targets


def _reset_all():
    for key in [
        "current_result",
        "current_error",
        "input_reset_counter",
    ]:
        if key in st.session_state:
            del st.session_state[key]

    st.session_state.input_reset_counter = st.session_state.get("input_reset_counter", 0) + 1


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

    errors = result.get("errors") or []

    if errors:
        with st.expander(f"Ver {len(errors)} errores"):
            for item in errors:
                st.write(
                    f"Trámite: `{item.get('dig_tramite')}` | "
                    f"ID Trámite: `{item.get('dig_id_tramite')}` | "
                    f"Cédula: `{item.get('cedula')}`"
                )
                st.code(item.get("error", ""))


def dashboard_page():
    _init_state()
    _render_css()

    st.markdown(
        """
        <div class="main-title">Cobertura automática MSP</div>
        <div class="main-subtitle">
            Genera coberturas desde FE_PLA_ANIOMES &ge; 202604,
            solo registros con DIG_COBERTURA='N' y DIG_PLANILLADO='S'.
            Actualiza DIG_COBERTURA='S' solo si el PDF existe.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="simple-card">', unsafe_allow_html=True)

    col1, col2 = st.columns([2, 1])

    with col1:
        generar = st.button(
            "Generar coberturas automáticas",
            key="generar_auto_button",
            use_container_width=True,
        )

    with col2:
        limpiar = st.button(
            "Limpiar",
            key="limpiar_auto_button",
            use_container_width=True,
        )

    if limpiar:
        _reset_all()
        st.rerun()

    progress_bar = st.empty()
    status_box = st.empty()
    detail_box = st.empty()

    if generar:
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
            elif estado.startswith("Esperando"):
                emoji = "⏳"
            else:
                emoji = "⚙️"

            status_box.markdown(
                f"""
                <div class="status-info">
                    {emoji} Procesando {done} de {total}<br>
                    Avance: {percent}%
                </div>
                """,
                unsafe_allow_html=True,
            )

            detail_box.info(
                f"Mes: {item.get('fe_pla_aniomes')} | "
                f"Trámite: {item.get('dig_tramite')} | "
                f"Cédula: {item.get('dig_cedula')} | "
                f"Estado: {estado}"
            )

        try:
            result = generar_coberturas_automaticas_desde_mes(
                username=st.session_state.oracle_user,
                password=st.session_state.oracle_password,
                progress_callback=on_progress,
            )

            progress_widget.progress(100)

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
