# =========================
# app.py — MODO AUTOMATIZADO
# Login directo con DIGITALIZACION desde .env
# =========================

import os
import time

import streamlit as st
from dotenv import load_dotenv

from src.oracle_jdbc import login_with_role, test_login
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


UI_REQUIRED_ROLE = os.environ.get("APP_UI_REQUIRED_ROLE", "SPD_ADMIN").strip() or "SPD_ADMIN"


LOGIN_MAX_FAILS = 3
LOGIN_COOLDOWN_SECONDS = 30


def _login_error_message(result: dict) -> str:
    """Traduce el error técnico de Oracle a un mensaje claro para el usuario."""
    if result.get("reason") == "no_role":
        return (
            f"Tu usuario es válido, pero no tiene permiso para esta aplicación "
            f"(requiere el rol {UI_REQUIRED_ROLE}). Solicítalo al administrador."
        )

    raw = (result.get("error") or "").upper()

    if "ORA-01017" in raw:
        return "Usuario o contraseña incorrectos. Revísalos e inténtalo de nuevo."
    if "ORA-28000" in raw:
        return "Tu cuenta está bloqueada por intentos fallidos. Contacta al administrador de base de datos."
    if "ORA-28001" in raw or "ORA-28002" in raw:
        return "Tu contraseña de Oracle caducó. Cámbiala y vuelve a ingresar."
    if "NINGÚN NODO" in raw or "ORA-12" in raw or "IO ERROR" in raw:
        return "No se pudo conectar con la base de datos. Inténtalo en unos minutos o avisa a soporte."

    return "No fue posible ingresar. Inténtalo de nuevo o avisa a soporte."


def _ui_login_gate():
    """Acceso con usuario y clave de Oracle; solo entra quien tenga el rol SPD_ADMIN."""
    if st.session_state.get("ui_auth_ok"):
        return

    st.markdown(
        f"""
        <div style="max-width: 460px; margin: 6rem auto 1rem auto; padding: 2rem 1.5rem; border: 1px solid #dbe3ef; border-radius: 16px; background: white; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);">
            <div style="font-size: 1.5rem; font-weight: 800; text-align: center; margin-bottom: 0.4rem;">Acceso restringido</div>
            <div style="text-align: center; color: #475569;">Ingresa tu usuario y clave de Oracle (rol {UI_REQUIRED_ROLE}).</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Pausa breve tras varios fallos: evita bloquear la cuenta en Oracle por insistir.
    wait = st.session_state.get("login_locked_until", 0) - time.time()
    locked = wait > 0

    with st.form("ui_login_form", clear_on_submit=False):
        usuario = st.text_input("Usuario Oracle", placeholder="Ej: NOMBRE_APELLIDO").strip()
        clave = st.text_input("Contraseña Oracle", type="password")
        submitted = st.form_submit_button("Ingresar", use_container_width=True, disabled=locked)

    if locked:
        st.warning(f"Demasiados intentos fallidos. Espera {int(wait) + 1} s antes de reintentar.")
        time.sleep(1)
        st.rerun()

    if submitted:
        if not usuario or not clave:
            st.warning("Debes ingresar usuario y contraseña.")
        else:
            with st.spinner("Validando contra Oracle..."):
                result = login_with_role(usuario, clave, UI_REQUIRED_ROLE)
            if result["ok"]:
                st.session_state.ui_auth_ok = True
                st.session_state.ui_user = result["db_user"]
                st.session_state.login_fails = 0
                st.rerun()
            else:
                fails = st.session_state.get("login_fails", 0) + 1
                st.session_state.login_fails = fails
                st.error(_login_error_message(result))
                if fails >= LOGIN_MAX_FAILS:
                    st.session_state.login_fails = 0
                    st.session_state.login_locked_until = time.time() + LOGIN_COOLDOWN_SECONDS
                    st.rerun()
                with st.expander("Detalle técnico"):
                    st.code(result["error"])

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
