---
name: cobertura-autonomia
description: Evalúa y mantiene la autonomía del worker, timer, locks, estado RUNNING/RETRY_PENDING/RETRY_PENDING_SLOW, cuarentena y reanudación automática. Úsalo cuando se pregunte si el sistema puede correr desatendido o por qué no avanza solo.
---

# Cobertura Automática MSP — Autonomía

## Componentes

- `scripts/resume_coberturas_auto.py`
- `scripts/run_resume_coberturas.sh`
- `src/auto_resume_state.py`
- `src/cobertura_runner.py`
- `src/quarantine.py`
- `src/operator_tools.py`
- `cobertura-auto-resume.timer`

## Estados esperados

- `RUNNING` / `RUNNING_BY_WORKER`
- `RETRY_PENDING` / `RETRY_PENDING_SLOW`
- `WAITING_OTHER_PROCESS`
- `COMPLETED`
- `STOPPED_BY_USER` / `PAUSED_BY_OPERATOR`

## Revisión rápida

```bash
cd /data_nuevo/cobertura_integrada
echo "== TIMER ==" && sudo systemctl list-timers | grep cobertura || true
echo "== STATUS ==" && sudo coberturactl status
echo "== STATE ==" && cat logs/cobertura_auto_resume_state.json 2>/dev/null || true
echo "== CUARENTENA ==" && sqlite3 logs/cobertura_quarantine.sqlite "SELECT COUNT(*) total, MAX(retry_count) max_reintentos FROM quarantine;" 2>/dev/null || true
echo "== PDFS RECIENTES ==" && find /data_nuevo/coberturas -type f -name "*.pdf" -mmin -60 | wc -l
echo "== LOGS ==" && du -sh logs
```

## Criterios para decir que corre solo

1. Timer activo.
2. Worker usa .venv o venv.
3. No abandona definitivamente por 5 errores.
4. `RETRY_PENDING_SLOW` sigue habilitado.
5. Cuarentena vence sola.
6. No necesita clic humano por cada lote.
7. Sync no se ejecuta completo sin necesidad.
8. Si no hay pendientes, queda en estado limpio.
9. Si hay corte, puede reanudarse.
10. Logs tienen retención o monitoreo.

## Riesgos

- Worker cada minuto sin backoff.
- Cuarentena de fallidos eternos.
- Sync completo frecuente sobre miles de carpetas.
- Estado RUNNING falso.
- Locks huérfanos.
- Logs y SQLite creciendo sin límite.

## Respuesta esperada

Cuando se pregunte por autonomía, responder con:
- porcentaje aproximado;
- riesgos restantes;
- comandos de verificación;
- cambios mínimos;
- rollback.
