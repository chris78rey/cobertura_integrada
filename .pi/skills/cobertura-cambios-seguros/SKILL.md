---
name: cobertura-cambios-seguros
description: Protocolo de cero roturas para modificar Cobertura Automática MSP. Úsalo antes de cambiar Python, Streamlit, Oracle, worker, cuarentena, sync, systemd, rutas o panel de operación.
---

# Cobertura Automática MSP — Cambios seguros

## Preparación obligatoria

```bash
cd /data_nuevo/cobertura_integrada
STAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p backups/$STAMP
cp -a src backups/$STAMP/
cp -a scripts backups/$STAMP/
cp -a config backups/$STAMP/
cp -a .streamlit backups/$STAMP/ 2>/dev/null || true
git diff > backups/$STAMP/git_diff_antes.patch || true
```

## Archivos de alto riesgo

- `src/cobertura_pdf.py`
- `src/oracle_jdbc.py`
- `scripts/sync_coberturas_repo.py`
- `scripts/resume_coberturas_auto.py`
- `src/auto_resume_state.py`
- `src/cobertura_runner.py`
- `src/quarantine.py`
- `scripts/coberturactl`
- Unidades systemd

## Validación técnica

```bash
cd /data_nuevo/cobertura_integrada
python3 -m py_compile app.py src/app_config.py src/auto_resume_state.py src/cobertura_pdf.py src/cobertura_runner.py src/operator_tools.py src/quarantine.py src/pages/dashboard.py scripts/resume_coberturas_auto.py
```

## Pruebas de regresión obligatorias

1. La app abre.
2. La ruta `/data_nuevo/coberturas` no se puede editar.
3. Generar por mes desde `202605` funciona.
4. Si no hay pendientes, no queda `RUNNING` falso.
5. Si hay lock activo, no se inicia otro proceso.
6. Oracle solo se actualiza si el PDF existe.
7. Sync no sobrescribe PDFs existentes.
8. Cuarentena reintenta sola.
9. Worker no abandona definitivamente por 5 errores.

## Rollback

```bash
cd /data_nuevo/cobertura_integrada
BACKUP_DIR=$(ls -td backups/*-cambio-seguro | head -1)
cp -a "$BACKUP_DIR/src/." src/
cp -a "$BACKUP_DIR/scripts/." scripts/
cp -a "$BACKUP_DIR/config/." config/
sudo coberturactl restart
sudo coberturactl status
```
