---
name: cobertura-revision-diaria
description: Rutina diaria de revisión del sistema. Estado completo en un solo paso: Oracle, worker, timer, PDFs, cuarentena, sync y logs recientes. Úsalo al inicio del día o cuando quieras saber si todo está bien.
---

# Cobertura Automática MSP — Revisión diaria

## Un solo comando para ver todo

```bash
cd /data_nuevo/cobertura_integrada

echo "========================================="
echo "  REVISIÓN DIARIA $(date '+%Y-%m-%d %H:%M')"
echo "========================================="

echo ""
echo "=== 1. TIMER ==="
sudo systemctl is-active cobertura-auto-resume.timer 2>/dev/null || echo "INACTIVO"
sudo systemctl status cobertura-auto-resume.timer --no-pager 2>/dev/null | grep -E "Active|Trigger" || true

echo ""
echo "=== 2. WORKER ==="
cat logs/cobertura_auto_resume_state.json 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  Estado: {d.get('status','?')}\")
print(f\"  Mes desde: {d.get('fe_pla_aniomes_desde','?')}\")
print(f\"  Pendientes antes/después: {d.get('pendientes_antes','?')} / {d.get('pendientes_despues','?')}\")
print(f\"  Última pasada: gen={d.get('last_generados','?')} act={d.get('last_actualizados','?')} err={d.get('last_errores','?')}\")
print(f\"  Actualizado: {d.get('updated_at','?')}\")
print(f\"  Error: {d.get('last_error','')[:100]}\")
" 2>/dev/null || echo "  Sin estado"

echo ""
echo "=== 3. ORACLE ==="
python3 -c "
import os; os.environ['TZ']='America/Bogota'
from src.oracle_jdbc import oracle_connect
c = oracle_connect('DIGITALIZACION','DIGITALIZACION')
for q, label in [
    (\"SELECT COUNT(*) FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(DIG_PLANILLADO)='S'\", 'Planillados totales'),
    (\"SELECT COUNT(*) FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(DIG_PLANILLADO)='S' AND TRIM(DIG_COBERTURA)='S'\", 'Generados (S)'),
    (\"SELECT COUNT(*) FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(DIG_PLANILLADO)='S' AND NVL(TRIM(DIG_COBERTURA),'N')='N'\", 'Pendientes (N)'),
    (\"SELECT COUNT(*) FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(DIG_PLANILLADO)='S' AND TRIM(FE_PLA_ANIOMES)='202605'\", '  → Solo 202605'),
    (\"SELECT COUNT(*) FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(DIG_PLANILLADO)='S' AND TRIM(FE_PLA_ANIOMES)='202605' AND TRIM(DIG_COBERTURA)='S'\", '  → 202605 generados'),
]:
    s = c.jconn.createStatement(); r = s.executeQuery(q); r.next()
    print(f'  {label}: {r.getInt(1)}')
c.close()
" 2>&1

echo ""
echo "=== 4. PDFs LOCALES ==="
echo "  Total: $(find /data_nuevo/coberturas -name '*.pdf' -type f 2>/dev/null | wc -l)"
echo "  Última hora: $(find /data_nuevo/coberturas -name '*.pdf' -type f -mmin -60 2>/dev/null | wc -l)"
echo "  Carpetas: $(ls -1 /data_nuevo/coberturas 2>/dev/null | wc -l)"

echo ""
echo "=== 5. ERRORES DE HOY ==="
grep -c "PDF_GENERATION_ERROR" logs/cobertura_auto_$(date +%Y%m%d)_*_errors.jsonl 2>/dev/null | awk -F: '{s+=$NF} END {print "  Errores hoy: " s}' || echo "  Sin errores"

echo ""
echo "=== 6. CUARENTENA ==="
sqlite3 logs/cobertura_quarantine.sqlite "SELECT COUNT(*) || ' activos, ' || COALESCE(MAX(retry_count),0) || ' max reintentos' FROM quarantine WHERE expires_at > strftime('%s','now');" 2>/dev/null || echo "  Sin cuarentena"

echo ""
echo "=== 7. STREAMLIT ==="
ps aux | grep "streamlit run app.py" | grep -v grep | awk '{print "  PID: " $2 " CPU: " $3 "% MEM: " $4 "%"}' || echo "  No está corriendo"

echo ""
echo "========================================="
echo "  FIN REVISIÓN"
echo "========================================="
```

Guardalo como `~/revision.sh` y ejecutalo con `bash ~/revision.sh`.

## Si algo falla

| Síntoma | Revisar |
|---|---|
| Timer inactivo | `sudo systemctl start cobertura-auto-resume.timer` |
| Worker sin estado | `bash scripts/run_resume_coberturas.sh` |
| Muchos pendientes | Revisar lock: `ls /tmp/cobertura_auto.lock` |
| Errores 503 | RSC caído, esperar — el worker reintenta solo |
| Streamlit caído | `sudo coberturactl restart` |
