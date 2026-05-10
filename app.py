# =========================
# app.py — MODO AUTOMATIZADO
# Login directo con DIGITALIZACION desde .env
# =========================

import os

import streamlit as st
from dotenv import load_dotenv

from src.oracle_jdbc import test_login
from src.pages.dashboard import dashboard_page
from src.pages.cedulas_tramite import cedulas_tramite_page
from src.pages.auditoria_menor_edad import auditoria_menor_edad_page
from src.ui import inject_global_css


load_dotenv()

st.set_page_config(
    page_title="Cobertura Automática MSP",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={},
)


def _auto_login():
    if st.session_state.get("auth_ok"):
        return

    user = os.environ.get("ORACLE_AUTO_USER", "").strip()
    password = os.environ.get("ORACLE_AUTO_PASSWORD", "").strip()

    if not user or not password:
        st.error("Faltan ORACLE_AUTO_USER u ORACLE_AUTO_PASSWORD en .env")
        st.stop()
        return

    result = test_login(user, password)

    if result["ok"]:
        st.session_state.auth_ok = True
        st.session_state.oracle_user = user
        st.session_state.oracle_password = password
        st.session_state.db_user = result["db_user"]
    else:
        st.error(f"No se pudo autenticar con el usuario {user}")
        st.code(result["error"])
        st.stop()


def _ui_login_gate():
    if st.session_state.get("ui_auth_ok"):
        return

    ui_password = os.environ.get("APP_UI_PASSWORD", "cr19780302").strip()

    st.markdown(
        """
        <div style="max-width: 460px; margin: 8rem auto 2rem auto; padding: 2rem 1.5rem; border: 1px solid #dbe3ef; border-radius: 16px; background: white; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);">
            <div style="font-size: 1.5rem; font-weight: 800; text-align: center; margin-bottom: 0.4rem;">Acceso restringido</div>
            <div style="text-align: center; color: #475569; margin-bottom: 1.2rem;">Ingresa la clave para abrir la aplicación.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.form("ui_login_form", clear_on_submit=False):
        clave = st.text_input("Clave de acceso", type="password")
        submitted = st.form_submit_button("Ingresar", use_container_width=True)

    if submitted:
        if clave.strip() == ui_password:
            st.session_state.ui_auth_ok = True
            st.rerun()
        else:
            st.error("Clave incorrecta.")

    st.stop()


def main():
    inject_global_css()
    _ui_login_gate()
    _auto_login()

    pagina = st.radio(
        "",
        ["🏥 Coberturas automáticas", "📝 Corrección de cédulas", "🔎 Auditoría menores"],
        horizontal=True,
        label_visibility="collapsed",
    )

    if pagina == "📝 Corrección de cédulas":
        cedulas_tramite_page()
    elif pagina == "🔎 Auditoría menores":
        auditoria_menor_edad_page()
    else:
        dashboard_page()


if __name__ == "__main__":
    main()
