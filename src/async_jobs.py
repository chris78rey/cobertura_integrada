# =========================
# EJECUCIÓN EN SEGUNDO PLANO + ESTADO VISIBLE
# =========================

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime, timezone

import streamlit as st

from src.config import get_max_workers


@st.cache_resource
def get_executor() -> ThreadPoolExecutor:
    """
    Executor global.

    El executor se puede compartir.
    Las conexiones Oracle NO se comparten.
    Cada tarea abre y cierra su propia conexión.
    """
    return ThreadPoolExecutor(max_workers=get_max_workers())


def submit_job(job_name: str, func, *args, timeout_seconds: int = 30, **kwargs) -> Future:
    executor = get_executor()
    future = executor.submit(func, *args, **kwargs)

    st.session_state.current_future = future
    st.session_state.current_job_name = job_name
    st.session_state.current_result = None
    st.session_state.current_error = None
    st.session_state.current_timeout_seconds = int(timeout_seconds)
    st.session_state.current_started_at_utc = datetime.now(timezone.utc).isoformat()
    st.session_state.current_timed_out_ui = False

    return future


def _elapsed_seconds() -> int:
    started_raw = st.session_state.get("current_started_at_utc")

    if not started_raw:
        return 0

    try:
        started_at = datetime.fromisoformat(started_raw)
        now = datetime.now(timezone.utc)
        return int((now - started_at).total_seconds())
    except Exception:
        return 0


def render_current_job():
    """
    Renderiza el estado de la tarea actual.
    No bloquea la pantalla esperando el resultado.
    """

    future: Future | None = st.session_state.get("current_future")

    if not future:
        return None

    job_name = st.session_state.get("current_job_name", "Proceso")
    timeout_seconds = int(st.session_state.get("current_timeout_seconds", 30))
    elapsed = _elapsed_seconds()

    if future.running():
        st.info(
            f"{job_name} en ejecución. "
            f"Tiempo transcurrido: {elapsed}s / timeout: {timeout_seconds}s."
        )

        progress_value = min(elapsed / max(timeout_seconds, 1), 1.0)
        st.progress(progress_value)

        if elapsed >= timeout_seconds:
            st.session_state.current_timed_out_ui = True
            st.warning(
                "El proceso superó el tiempo esperado. "
                "La interfaz sigue activa y el driver JDBC intentará controlar la operación."
            )

        if st.button(
            "Actualizar estado",
            key="async_job_refresh_button",
            use_container_width=True,
        ):
            st.rerun()

        return None

    if future.done():
        try:
            result = future.result()
            st.session_state.current_result = result
            st.session_state.current_error = None
            st.session_state.current_timed_out_ui = False
            st.success(f"{job_name} finalizado correctamente en {elapsed}s.")
            return result

        except Exception as exc:
            st.session_state.current_result = None
            st.session_state.current_error = str(exc)
            st.error(f"{job_name} terminó con error.")
            st.code(str(exc))
            return None

    return None

# =========================
# FIN
# =========================
