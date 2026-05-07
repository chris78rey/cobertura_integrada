from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from src.oracle_jdbc import oracle_connect
from src.repo_sync import ejecutar_sync_repo


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")
OUTPUT_ROOT = Path("/data_nuevo/coberturas")
REPO_ROOT = Path("/data_nuevo/repo_grande/data/datos")
LOGS_DIR = PROJECT_ROOT / "logs"
AUDIT_LOG = LOGS_DIR / "auditoria_menor_edad_sync.jsonl"
BACKUP_CC_ROOT = LOGS_DIR / "backup_auditoria_menor_edad_cc"
PDF_CC_REGEX = re.compile(r"^CC(?:_\d{2})?\.pdf$", re.IGNORECASE)
PDF_CC_LEGACY_REGEX = re.compile(r"^CC(?:\d+)?\.pdf$", re.IGNORECASE)


def _ahora_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ahora_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _auditar_evento(evento: dict[str, Any]) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"ts": _ahora_iso(), **evento}
    with AUDIT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _validar_mes(valor: str) -> str:
    valor = str(valor or "").strip()
    if not re.fullmatch(r"\d{6}", valor):
        raise ValueError("FE_PLA_ANIOMES debe tener formato AAAAMM, por ejemplo 202605.")
    mes = int(valor[4:6])
    if mes < 1 or mes > 12:
        raise ValueError("FE_PLA_ANIOMES tiene un mes inválido.")
    return valor


def _validar_tramite(valor: str) -> str:
    valor = str(valor or "").strip()
    if not valor or not valor.isdigit():
        raise ValueError("El trámite debe contener solo números.")
    if len(valor) > 30:
        raise ValueError("El trámite no debe superar 30 dígitos.")
    return valor


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _is_pdf_cc(path: Path) -> bool:
    return path.is_file() and PDF_CC_REGEX.fullmatch(path.name) is not None


def _is_pdf_cc_legacy(path: Path) -> bool:
    return path.is_file() and PDF_CC_LEGACY_REGEX.fullmatch(path.name) is not None and not _is_pdf_cc(path)


def _listar_cc(path: Path) -> list[str]:
    if not path.exists() or not path.is_dir():
        return []
    return sorted(p.name for p in path.iterdir() if _is_pdf_cc(p))


def _listar_cc_legacy(path: Path) -> list[str]:
    if not path.exists() or not path.is_dir():
        return []
    return sorted(p.name for p in path.iterdir() if _is_pdf_cc_legacy(p))


def _consultar_menores_por_mes(
    username: str,
    password: str,
    fe_pla_aniomes: str,
    max_rows: int,
) -> list[dict[str, str]]:
    conn = ps = rs = None
    sql = """
        SELECT *
        FROM (
            SELECT
                TO_CHAR(DIG_TRAMITE) AS DIG_TRAMITE,
                TRIM(NVL(DIG_CEDULA, '')) AS DIG_CEDULA,
                TRIM(NVL(DIG_DEPENDIENTE_01, '')) AS DIG_DEPENDIENTE_01,
                TRIM(NVL(DIG_DEPENDIENTE_02, '')) AS DIG_DEPENDIENTE_02,
                TRIM(NVL(DIG_MENOR_EDAD, 'N')) AS DIG_MENOR_EDAD,
                TRIM(NVL(DIG_COBERTURA, 'N')) AS DIG_COBERTURA,
                TRIM(NVL(DIG_PLANILLADO, '')) AS DIG_PLANILLADO,
                TRIM(TO_CHAR(FE_PLA_ANIOMES)) AS FE_PLA_ANIOMES
            FROM DIGITALIZACION.DIGITALIZACION
            WHERE TRIM(TO_CHAR(FE_PLA_ANIOMES)) = ?
              AND TRIM(NVL(DIG_MENOR_EDAD, 'N')) = 'S'
              AND TRIM(NVL(DIG_PLANILLADO, 'N')) = 'S'
            ORDER BY DIG_TRAMITE
        )
        WHERE ROWNUM <= ?
    """
    rows: list[dict[str, str]] = []
    try:
        conn = oracle_connect(username, password)
        ps = conn.jconn.prepareStatement(sql)
        ps.setString(1, fe_pla_aniomes)
        ps.setInt(2, int(max_rows))
        ps.setQueryTimeout(90)
        rs = ps.executeQuery()
        while rs.next():
            rows.append({
                "DIG_TRAMITE": str(rs.getString(1) or "").strip(),
                "DIG_CEDULA": str(rs.getString(2) or "").strip(),
                "DIG_DEPENDIENTE_01": str(rs.getString(3) or "").strip(),
                "DIG_DEPENDIENTE_02": str(rs.getString(4) or "").strip(),
                "DIG_MENOR_EDAD": str(rs.getString(5) or "").strip(),
                "DIG_COBERTURA": str(rs.getString(6) or "").strip(),
                "DIG_PLANILLADO": str(rs.getString(7) or "").strip(),
                "FE_PLA_ANIOMES": str(rs.getString(8) or "").strip(),
            })
        return rows
    finally:
        for obj in (rs, ps, conn):
            if obj:
                try:
                    obj.close()
                except Exception:
                    pass


@st.cache_data(ttl=300, show_spinner=False)
def _construir_indice_destinos_por_year(repo_root_text: str, year: str) -> dict[str, list[str]]:
    repo_root = Path(repo_root_text).resolve()
    scope = repo_root / year if year and (repo_root / year).exists() else repo_root
    indice: dict[str, list[str]] = {}
    if not scope.exists():
        return indice

    for path in scope.rglob("*"):
        if not path.is_dir():
            continue
        if not re.fullmatch(r"\d{1,30}", path.name):
            continue
        resolved = str(path.resolve())
        if resolved.startswith(str(scope.resolve())):
            indice.setdefault(path.name, []).append(resolved)

    return {k: sorted(v) for k, v in indice.items()}


def _auditar_fila(row: dict[str, str], indice_destinos: dict[str, list[str]]) -> dict[str, Any]:
    tramite = str(row.get("DIG_TRAMITE", "")).strip()
    local_dir = OUTPUT_ROOT / tramite
    destinos = [Path(p) for p in indice_destinos.get(tramite, [])]
    destino_dir = destinos[0] if len(destinos) == 1 else None

    local_cc = _listar_cc(local_dir)
    destino_cc = _listar_cc(destino_dir) if destino_dir else []
    legacy_destino = _listar_cc_legacy(destino_dir) if destino_dir else []

    esperado = ["CC_01.pdf", "CC_02.pdf", "CC_03.pdf"]
    faltan_local = [x for x in esperado if x not in local_cc]
    faltan_destino = [x for x in esperado if x not in destino_cc]
    extras_local = [x for x in local_cc if x not in esperado]
    extras_destino = [x for x in destino_cc if x not in esperado]

    cedulas = [
        str(row.get("DIG_CEDULA", "")).strip(),
        str(row.get("DIG_DEPENDIENTE_01", "")).strip(),
        str(row.get("DIG_DEPENDIENTE_02", "")).strip(),
    ]
    oracle_completo = all(cedulas)

    if not oracle_completo:
        estado = "ORACLE_INCOMPLETO"
        accion = "Corregir cédulas en Oracle."
    elif not local_dir.exists():
        estado = "SIN_CARPETA_LOCAL"
        accion = "Regenerar cobertura local."
    elif faltan_local:
        estado = "FALTAN_CC_LOCALES"
        accion = "Regenerar cobertura local antes de sincronizar."
    elif len(destinos) == 0:
        estado = "DESTINO_NO_EXISTE"
        accion = "La carpeta destino todavía no existe."
    elif len(destinos) > 1:
        estado = "DESTINO_AMBIGUO"
        accion = "Revisar carpetas destino duplicadas."
    elif legacy_destino:
        estado = "DESTINO_LEGACY_CC"
        accion = "Sincronizar para limpiar CC legacy y dejar solo CC_01/02/03."
    elif not faltan_destino and not extras_destino:
        estado = "OK_DESTINO"
        accion = "Sin acción."
    else:
        estado = "REQUIERE_SINCRONIZAR"
        accion = "Sincronizar solo CC*.pdf."

    return {
        **row,
        "ESTADO_AUDITORIA": estado,
        "ACCION_RECOMENDADA": accion,
        "ESPERADOS": ", ".join(esperado),
        "CC_LOCAL": ", ".join(local_cc),
        "CC_DESTINO": ", ".join(destino_cc),
        "CC_LEGACY_DESTINO": ", ".join(legacy_destino),
        "FALTAN_LOCAL": ", ".join(faltan_local),
        "FALTAN_DESTINO": ", ".join(faltan_destino),
        "EXTRAS_LOCAL": ", ".join(extras_local),
        "EXTRAS_DESTINO": ", ".join(extras_destino),
        "DESTINO_DIR": str(destino_dir) if destino_dir else "",
        "PUEDE_SINCRONIZAR": estado in {"REQUIERE_SINCRONIZAR", "DESTINO_LEGACY_CC"},
    }


def _backup_y_reemplazar_solo_cc(destino_dir: Path, tramite: str, nuevos_pdfs: list[Path], usuario: str) -> dict[str, Any]:
    destino_dir = destino_dir.resolve()
    repo_root = REPO_ROOT.resolve()
    if not str(destino_dir).startswith(str(repo_root)):
        raise RuntimeError(f"Destino fuera del repositorio oficial: {destino_dir}")
    if not destino_dir.exists() or not destino_dir.is_dir():
        raise RuntimeError(f"No existe carpeta destino: {destino_dir}")
    if not nuevos_pdfs:
        raise RuntimeError("No hay PDFs CC*.pdf nuevos")

    for src_pdf in nuevos_pdfs:
        if not _is_pdf_cc(src_pdf):
            raise RuntimeError(f"Archivo no permitido: {src_pdf.name}")
        if src_pdf.stat().st_size <= 0:
            raise RuntimeError(f"PDF local vacío: {src_pdf}")

    run_id = _ahora_id()
    backup_dir = BACKUP_CC_ROOT / tramite / run_id
    staging_dir = backup_dir / "nuevos_verificados"
    backup_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)
    existentes_cc = sorted([p for p in destino_dir.iterdir() if _is_pdf_cc(p)])
    legacy_cc = sorted([p for p in destino_dir.iterdir() if _is_pdf_cc_legacy(p)])
    reemplazables = sorted(existentes_cc + legacy_cc, key=lambda p: p.name)

    manifest: dict[str, Any] = {
        "run_id": run_id,
        "usuario": usuario,
        "tramite": tramite,
        "destino_dir": str(destino_dir),
        "backup_dir": str(backup_dir),
        "staging_dir": str(staging_dir),
        "existentes_respaldados": [],
        "legacy_respaldados": [],
        "nuevos_verificados": [],
        "eliminados_destino": [],
        "copiados_nuevos": [],
        "restauracion_por_error": [],
        "otros_archivos_no_tocados": [],
    }

    for item in sorted(destino_dir.iterdir()):
        if item.is_file() and not _is_pdf_cc(item):
            manifest["otros_archivos_no_tocados"].append(item.name)

    for old_pdf in reemplazables:
        backup_pdf = backup_dir / old_pdf.name
        shutil.copy2(old_pdf, backup_pdf)
        entry = {
            "archivo": old_pdf.name,
            "origen": str(old_pdf),
            "backup": str(backup_pdf),
            "sha256": _sha256_file(backup_pdf),
        }
        if _is_pdf_cc_legacy(old_pdf):
            manifest["legacy_respaldados"].append(entry)
        else:
            manifest["existentes_respaldados"].append(entry)

    for src_pdf in nuevos_pdfs:
        staged_pdf = staging_dir / src_pdf.name
        shutil.copy2(src_pdf, staged_pdf)
        src_hash = _sha256_file(src_pdf)
        staged_hash = _sha256_file(staged_pdf)
        if src_hash != staged_hash:
            raise RuntimeError(f"Hash no coincide en staging para {src_pdf.name}")
        manifest["nuevos_verificados"].append({
            "archivo": src_pdf.name,
            "origen": str(src_pdf),
            "staging": str(staged_pdf),
            "sha256": staged_hash,
        })

    try:
        for old_pdf in reemplazables:
            old_pdf.unlink()
            manifest["eliminados_destino"].append(str(old_pdf))
        for staged_pdf in sorted(staging_dir.iterdir()):
            if not _is_pdf_cc(staged_pdf):
                continue
            dst_pdf = destino_dir / staged_pdf.name
            shutil.copy2(staged_pdf, dst_pdf)
            staged_hash = _sha256_file(staged_pdf)
            dst_hash = _sha256_file(dst_pdf)
            if staged_hash != dst_hash:
                raise RuntimeError(f"Hash no coincide luego de copiar {staged_pdf.name}")
            manifest["copiados_nuevos"].append({
                "archivo": staged_pdf.name,
                "origen": str(staged_pdf),
                "destino": str(dst_pdf),
                "sha256": dst_hash,
            })
    except Exception as exc:
        for current_cc in sorted([p for p in destino_dir.iterdir() if _is_pdf_cc(p)]):
            try:
                current_cc.unlink()
            except Exception:
                pass
        for item in manifest["existentes_respaldados"] + manifest["legacy_respaldados"]:
            backup_pdf = Path(item["backup"])
            restore_pdf = destino_dir / backup_pdf.name
            try:
                shutil.copy2(backup_pdf, restore_pdf)
                manifest["restauracion_por_error"].append({
                    "archivo": backup_pdf.name,
                    "restaurado": str(restore_pdf),
                })
            except Exception as restore_exc:
                manifest["restauracion_por_error"].append({
                    "archivo": backup_pdf.name,
                    "error_restaurando": str(restore_exc),
                })
        manifest_path_error = backup_dir / "manifest_auditoria_menor_edad_error.json"
        manifest_path_error.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError(f"Falló la sincronización de CC*.pdf. Se intentó restaurar. Detalle: {exc}") from exc

    manifest_path = backup_dir / "manifest_auditoria_menor_edad.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    _auditar_evento({
        "evento": "SYNC_MENOR_EDAD_CC_OK",
        "usuario": usuario,
        "tramite": tramite,
        "manifest": str(manifest_path),
    })

    return {
        "ok": True,
        "run_id": run_id,
        "backup_dir": str(backup_dir),
        "manifest_path": str(manifest_path),
        "eliminados": len(manifest["eliminados_destino"]),
        "copiados": len(manifest["copiados_nuevos"]),
        "otros_no_tocados": len(manifest["otros_archivos_no_tocados"]),
    }


def _sincronizar_item(row: dict[str, Any], username: str) -> dict[str, Any]:
    tramite = str(row.get("DIG_TRAMITE", "")).strip()
    destino_dir = Path(str(row.get("DESTINO_DIR", "")).strip())
    local_dir = OUTPUT_ROOT / tramite
    nuevos_pdfs = [local_dir / "CC_01.pdf", local_dir / "CC_02.pdf", local_dir / "CC_03.pdf"]
    if not all(p.exists() and p.stat().st_size > 0 for p in nuevos_pdfs):
        raise RuntimeError(f"No están completos los tres PDFs locales esperados en {local_dir}")
    return _backup_y_reemplazar_solo_cc(
        destino_dir=destino_dir,
        tramite=tramite,
        nuevos_pdfs=nuevos_pdfs,
        usuario=username,
    )


def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, quoting=csv.QUOTE_MINIMAL)
    return buffer.getvalue().encode("utf-8-sig")


def auditoria_menor_edad_page() -> None:
    st.markdown(
        """
        <div class="main-title">Auditoría de menores de edad</div>
        <div class="main-subtitle">Filtro obligatorio por FE_PLA_ANIOMES y sincronización segura de solo CC*.pdf.</div>
        """,
        unsafe_allow_html=True,
    )

    st.warning(
        "Esta pantalla no modifica Oracle ni regenera PDFs. Solo sincroniza CC*.pdf cuando el trámite ya tiene los tres archivos locales.",
        icon="⚠️",
    )

    username = st.session_state.get("oracle_user", "")
    password = st.session_state.get("oracle_password", "")
    if not username or not password:
        st.error("No hay sesión de Oracle activa.")
        return

    st.markdown('<div class="simple-card">', unsafe_allow_html=True)
    col_mes, col_limite = st.columns([2, 1])
    with col_mes:
        fe_pla_aniomes = st.text_input(
            "FE_PLA_ANIOMES",
            value=st.session_state.get("audit_menor_mes", "202605"),
            max_chars=6,
        )
    with col_limite:
        max_rows = st.number_input(
            "Máximo registros",
            min_value=10,
            max_value=5000,
            value=1000,
            step=100,
        )

    solo_problemas = st.checkbox("Mostrar solo trámites con problema", value=True)

    col_buscar, col_limpiar = st.columns([3, 1])
    with col_buscar:
        buscar = st.button("Buscar menores del mes", use_container_width=True, key="btn_audit_menor_buscar")
    with col_limpiar:
        if st.button("Limpiar", use_container_width=True, key="btn_audit_menor_limpiar"):
            for key in ["audit_menor_rows", "audit_menor_mes", "audit_menor_selected"]:
                st.session_state.pop(key, None)
            st.rerun()

    if buscar:
        try:
            mes = _validar_mes(fe_pla_aniomes)
            st.session_state["audit_menor_mes"] = mes
            with st.spinner("Consultando Oracle y revisando destino..."):
                oracle_rows = _consultar_menores_por_mes(username, password, mes, int(max_rows))
                indice_destinos = _construir_indice_destinos_por_year(str(REPO_ROOT), mes[:4])
                auditados = [_auditar_fila(row, indice_destinos) for row in oracle_rows]
            st.session_state["audit_menor_rows"] = auditados
        except Exception as exc:
            st.error(str(exc))
            st.markdown("</div>", unsafe_allow_html=True)
            return

    rows = st.session_state.get("audit_menor_rows", [])
    if not rows:
        st.info("Ingresar FE_PLA_ANIOMES, por ejemplo 202605, y presionar Buscar menores del mes.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    df = pd.DataFrame(rows)
    df_view = df.copy()
    if solo_problemas:
        df_view = df_view[df_view["ESTADO_AUDITORIA"] != "OK_DESTINO"]

    total = len(df)
    problemas = int((df["ESTADO_AUDITORIA"] != "OK_DESTINO").sum())
    sincronizables = int(df["PUEDE_SINCRONIZAR"].sum())
    ok = total - problemas

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Menores revisados", total)
    c2.metric("OK destino", ok)
    c3.metric("Con problema", problemas)
    c4.metric("Sincronizables", sincronizables)

    columnas = [
        "DIG_TRAMITE",
        "FE_PLA_ANIOMES",
        "DIG_CEDULA",
        "DIG_DEPENDIENTE_01",
        "DIG_DEPENDIENTE_02",
        "DIG_COBERTURA",
        "ESTADO_AUDITORIA",
        "CC_LEGACY_DESTINO",
        "FALTAN_DESTINO",
        "EXTRAS_DESTINO",
        "FALTAN_LOCAL",
        "DESTINO_DIR",
        "ACCION_RECOMENDADA",
    ]

    st.dataframe(df_view[columnas], use_container_width=True, hide_index=True)

    st.download_button(
        "Descargar reporte CSV",
        data=_df_to_csv_bytes(df_view[columnas]),
        file_name=f"auditoria_menor_edad_{st.session_state.get('audit_menor_mes', '')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    candidatos = [r for r in rows if r.get("PUEDE_SINCRONIZAR")]
    if not candidatos:
        st.info("No hay trámites sincronizables. Los restantes requieren regeneración local o corrección Oracle.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    opciones = {
        f"{r['DIG_TRAMITE']} | faltan: {r.get('FALTAN_DESTINO','') or '-'} | extras: {r.get('EXTRAS_DESTINO','') or '-'}": r
        for r in candidatos
    }

    st.markdown("---")
    st.markdown("### Sincronización segura")

    seleccion = st.multiselect(
        "Seleccionar trámites a sincronizar",
        options=list(opciones.keys()),
        key="audit_menor_selected",
    )

    with st.form("form_auditoria_menor_sync"):
        confirmar = st.checkbox("Confirmo sincronizar únicamente CC*.pdf de los trámites seleccionados")
        frase = st.text_input("Escribir SINCRONIZAR para confirmar", value="")
        submitted = st.form_submit_button("Sincronizar CC*.pdf seleccionados", use_container_width=True)

        if submitted:
            if not seleccion:
                st.warning("Debe seleccionar al menos un trámite.")
                st.markdown("</div>", unsafe_allow_html=True)
                return
            if not confirmar or frase.strip().upper() != "SINCRONIZAR":
                st.warning("Falta confirmar y escribir SINCRONIZAR.")
                st.markdown("</div>", unsafe_allow_html=True)
                return

            resultados = []
            try:
                for etiqueta in seleccion:
                    row = opciones[etiqueta]
                    try:
                        res = _sincronizar_item(row, username)
                        resultados.append({"DIG_TRAMITE": row["DIG_TRAMITE"], "ok": True, **res})
                    except Exception as exc:
                        _auditar_evento({
                            "evento": "SYNC_MENOR_EDAD_CC_ERROR",
                            "usuario": username,
                            "tramite": row.get("DIG_TRAMITE"),
                            "error": str(exc),
                        })
                        resultados.append({
                            "DIG_TRAMITE": row.get("DIG_TRAMITE"),
                            "ok": False,
                            "error": str(exc),
                        })
            except Exception as exc:
                st.error(str(exc))
                st.markdown("</div>", unsafe_allow_html=True)
                return

            st.success("Proceso de sincronización finalizado.")
            st.dataframe(pd.DataFrame(resultados), use_container_width=True, hide_index=True)
            _construir_indice_destinos_por_year.clear()
            st.session_state.pop("audit_menor_rows", None)
            st.info("Presionar Buscar menores del mes para refrescar el reporte.")

    st.markdown("</div>", unsafe_allow_html=True)
