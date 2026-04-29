# =========================
# app.py
# =========================

import streamlit as st

from src.auth import init_auth_state, login_screen, logout_button
from src.pages.dashboard import dashboard_page
from src.ui import inject_global_css


st.set_page_config(
    page_title="Sistema de Consulta Oracle",
    page_icon="🗄️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={},
)


def main():
    inject_global_css()
    init_auth_state()

    if not st.session_state.auth_ok:
        login_screen()
        return

    logout_button()
    dashboard_page()


if __name__ == "__main__":
    main()
