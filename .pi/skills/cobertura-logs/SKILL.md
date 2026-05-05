---
name: cobertura-logs
description: Cómo leer, interpretar y buscar en los logs del worker, generación de PDFs, sync y corrección de cédulas. Úsalo para diagnosticar errores, seguir un trámite o entender qué pasó.
---

# Cobertura Automática MSP — Logs y diagnóstico

## Estructura de logs

```
logs/
├── cobertura_auto_YYYYMMDD_HHMMSS.jsonl        ← Worker: todos los eventos
├── cobertura_auto_YYYYMMDD_HHMMSS_errors.jsonl  ← Worker: solo errores
├── cedulas_tramite_audit.jsonl                  ← Corrección de cédulas
├── cobertura_auto_resume_state.json             ← Estado persistente
├── cobertura_repo_sync.sqlite                   ← Sync: tracking
├── cobertura_sync_queue.sqlite                  ← Sync: cola pendiente
├── cobertura_quarantine.sqlite                  ← Cuarentena
└── worker_launcher.log                          ← Salida del launcher
```

## Buscar un trámite en todos los logs

```bash
cd /data_nuevo/cobertura_integrada
grep "5959391" logs/cobertura_auto_*.jsonl
```

## Ver últimas líneas del worker activo

```bash
tail -20 $(ls -t logs/cobertura_auto_*.jsonl | head -1)
```

## Filtrar solo eventos importantes

```bash
cat logs/cobertura_auto_20260505_163436.jsonl | python3 -c "
import sys, json
for line in sys.stdin:
    e = json.loads(line.strip())
    t = e.get('event','')
    if t in ('ROW_START','ORACLE_UPDATE_END','PDF_GENERATION_ERROR','RUN_START','RUN_END'):
        print(f\"{e['ts']} | {t} | tram={e.get('dig_tramite','')} | {e.get('total','')} | {e.get('ok','')}\")
"
```

## Errores de hoy

```bash
grep "PDF_GENERATION_ERROR" logs/cobertura_auto_$(date +%Y%m%d)_*.jsonl 2>/dev/null | head -20
```

## Auditoría de corrección de cédulas para un trámite

```bash
grep "5958993" logs/cedulas_tramite_audit.jsonl | python3 -c "
import sys, json
for line in sys.stdin:
    e = json.loads(line.strip())
    a = e['antes']; d = e['despues']
    print(f\"{e['ts']} | {e['evento']} | CED: {a['DIG_CEDULA']}->{d['DIG_CEDULA']} | COB: {a['DIG_COBERTURA']}->{d['DIG_COBERTURA']}\")
"
```

## Ver estado persistente

```bash
cat logs/cobertura_auto_resume_state.json | python3 -m json.tool
```

## Conteo de errores por categoría

```bash
grep "PDF_GENERATION_ERROR" logs/cobertura_auto_$(date +%Y%m%d)_*.jsonl 2>/dev/null | python3 -c "
import sys, json
from collections import Counter
cats = Counter()
for line in sys.stdin:
    e = json.loads(line.strip())
    err = e.get('error','')[:80]
    if '503' in err: cats['503 Service Unavailable'] += 1
    elif 'timeout' in err.lower(): cats['Timeout'] += 1
    else: cats['Otro'] += 1
for c, n in cats.most_common(): print(f'{c}: {n}')
"
```

## Ver cuarentena activa

```bash
sqlite3 logs/cobertura_quarantine.sqlite "SELECT tramite, retry_count, motivo, datetime(expires_at,'unixepoch','localtime') FROM quarantine WHERE expires_at > strftime('%s','now') ORDER BY expires_at LIMIT 20;"
```

## Ver cola de sync pendiente

```bash
sqlite3 logs/cobertura_sync_queue.sqlite "SELECT tramite, attempts, status, updated_at FROM cobertura_sync_queue WHERE status='PENDING' ORDER BY created_at LIMIT 20;"
```
