from pathlib import Path

import streamlit as st

from src.cobertura_pdf import generar_hojas_cobertura_por_id


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

        div[data-testid="stDownloadButton"] > button {
            background: #16a34a !important;
            color: white !important;
            border: 0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_result():
    result = st.session_state.get("current_result")

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

    zip_path_value = result.get("zip_path")

    if zip_path_value:
        zip_path = Path(zip_path_value)

        if zip_path.exists():
            with zip_path.open("rb") as file:
                st.download_button(
                    "Descargar ZIP de coberturas",
                    data=file,
                    file_name=zip_path.name,
                    mime="application/zip",
                    use_container_width=True,
                )
        else:
            st.warning("El ZIP fue generado, pero no se encontró el archivo en disco.")
    else:
        st.warning("No se generó ZIP porque no hubo PDFs disponibles.")

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


def dashboard_page():
    _init_state()
    _render_css()

    st.markdown(
        """
        <div class="main-title">Generar hojas de cobertura</div>
        <div class="main-subtitle">
            Ingrese el código de generación. El sistema generará los PDFs,
            mostrará el avance y dejará listo un ZIP para descargar.
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="simple-card">', unsafe_allow_html=True)

    input_key = f"cobertura_id_generacion_input_{st.session_state.input_reset_counter}"

    codigo_generacion = st.text_input(
        "Código de generación",
        placeholder="Ejemplo: 12345",
        key=input_key,
    )

    overwrite = st.checkbox(
        "Regenerar PDFs aunque ya existan",
        value=False,
    )

    col1, col2 = st.columns([2, 1])

    with col1:
        generar = st.button(
            "Generar ZIP",
            key="generar_zip_coberturas_button",
            use_container_width=True,
        )

    with col2:
        limpiar = st.button(
            "Limpiar",
            key="limpiar_coberturas_button",
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

        codigo_limpio = codigo_generacion.strip()

        if not codigo_limpio:
            st.warning("Ingrese el código de generación.")
        else:
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

                status_box.markdown(
                    f"""
                    <div class="status-info">
                        Procesando PDF {done} de {total}<br>
                        Avance: {percent}%
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                detail_box.info(
                    f"Planilla: {item.get('planilla')} | "
                    f"Cédula: {item.get('cedula')} | "
                    f"Fecha: {item.get('fecha')}"
                )

            try:
                result = generar_hojas_cobertura_por_id(
                    username=st.session_state.oracle_user,
                    password=st.session_state.oracle_password,
                    id_generacion=codigo_limpio,
                    overwrite=overwrite,
                    progress_callback=on_progress,
                    crear_zip=True,
                )

                progress_widget.progress(100)

                status_box.markdown(
                    """
                    <div class="status-success">
                        Proceso terminado. ZIP listo para descargar.
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                detail_box.empty()
                st.session_state.current_result = result

            except Exception as exc:
                st.session_state.current_error = str(exc)
                st.error("No se pudo generar el ZIP.")
                st.code(str(exc))

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
