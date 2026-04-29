# =========================
# app.py — MODO AUTOMATIZADO
# Login directo con DIGITALIZACION desde .env
# =========================

import os

import streamlit as st

from dotenv import load_dotenv

from src.oracle_jdbc import test_login
from src.pages.dashboard import dashboard_page
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


def main():
    inject_global_css()
    _auto_login()
    dashboard_page()


if __name__ == "__main__":
    main()
