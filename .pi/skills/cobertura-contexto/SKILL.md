---
name: cobertura-contexto
description: Carga el contexto estable del proyecto Cobertura Automática MSP: arquitectura, rutas oficiales, reglas de integridad, módulos principales y criterios que nunca deben romperse. Úsalo antes de analizar bugs, cambios, autonomía, sync, cuarentena u Oracle.
---

# Cobertura Automática MSP — Contexto estable

## Propósito

El sistema genera PDFs de cobertura MSP desde Oracle, guarda los PDFs en una ruta fija, actualiza Oracle solo cuando el PDF existe y sincroniza los PDFs generados hacia el repositorio oficial.

## Rutas oficiales

- Proyecto: `/data_nuevo/cobertura_integrada`
- Salida oficial de PDFs: `/data_nuevo/coberturas`
- Repositorio oficial destino: `/data_nuevo/repo_grande/data/datos`
- Logs: `/data_nuevo/cobertura_integrada/logs`
- Configuración: `/data_nuevo/cobertura_integrada/config`

## Reglas que nunca deben romperse

1. No actualizar `DIG_COBERTURA='S'` si el PDF no existe físicamente.
2. No sobrescribir PDFs existentes en el repositorio oficial.
3. No permitir que `/data_nuevo/coberturas` sea editable desde pantalla.
4. No ejecutar dos procesos de generación al mismo tiempo.
5. No matar procesos activos desde la UI.
6. No liberar locks sin verificar si hay proceso activo.
7. No convertir la cuarentena en un proceso manual de clics.
8. No usar `202604` como producción si fue mes de prueba.
9. Producción real inicia desde `202605`, salvo nueva instrucción formal.

## Módulos importantes

- `app.py`: entrada Streamlit.
- `src/pages/dashboard.py`: dashboard y panel de operación.
- `src/cobertura_pdf.py`: generación principal por lotes.
- `src/cobertura_runner.py`: lock global.
- `src/quarantine.py`: cuarentena persistente.
- `src/auto_resume_state.py`: estado del worker.
- `src/operator_tools.py`: herramientas seguras para operador.
- `scripts/resume_coberturas_auto.py`: worker automático.
- `scripts/sync_coberturas_repo.py`: sync al repo.
- `scripts/coberturactl`: comando operativo.
- `scripts/run_streamlit.sh`: arranque Streamlit.
- `scripts/run_resume_coberturas.sh`: arranque worker.

## Flujo oficial

1. Consulta Oracle: `FE_PLA_ANIOMES >= mes`, `DIG_COBERTURA='N'`, `DIG_PLANILLADO='S'`
2. Genera PDFs con Node.js + Playwright.
3. Guarda en `/data_nuevo/coberturas/{TRAMITE}/`.
4. Actualiza Oracle solo si el PDF existe.
5. Sincroniza al repositorio oficial sin sobrescribir.
6. Registra logs, errores, manifiestos, cuarentena y sync.

## Formato de toda respuesta técnica

Toda propuesta técnica debe incluir:
1. Impacto y riesgos.
2. Preparación con backups.
3. Implementación paso a paso.
4. Pruebas de verificación y regresión.
5. Plan de reversión.
