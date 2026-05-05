---
name: cobertura-worker-control
description: Control directo del worker sin depender de sudo ni systemd. Lanzar, detener, forzar corrida, cambiar mes, ver estado. Úsalo cuando sudo no está disponible o necesitás control fino.
---

# Cobertura Automática MSP — Control directo del worker

## Lanzar el worker manualmente

```bash
cd /data_nuevo/cobertura_integrada
bash scripts/run_resume_coberturas.sh
```

Esto ejecuta una pasada y termina. El timer lo vuelve a lanzar en el próximo ciclo.

## Dejarlo corriendo en segundo plano

```bash
cd /data_nuevo/cobertura_integrada
nohup bash scripts/run_resume_coberturas.sh > /dev/null 2>&1 &
echo "PID: $!"
```

## Ver si está corriendo

```bash
ps aux | grep "resume_coberturas_auto" | grep -v grep
```

## Matar el worker

```bash
pkill -f "resume_coberturas_auto.py"
```

## Ver el estado sin tocar nada

```bash
cat /data_nuevo/cobertura_integrada/logs/cobertura_auto_resume_state.json | python3 -m json.tool
```

## Cambiar el mes base (ej: a 202606)

```bash
cd /data_nuevo/cobertura_integrada
# Cambiar en .env
sed -i 's/AUTO_FE_PLA_ANIOMES_DESDE=.*/AUTO_FE_PLA_ANIOMES_DESDE=202606/' .env

# Limpiar estado para que arranque fresco
cat logs/cobertura_auto_resume_state.json | python3 -c "
import sys, json
d = json.load(sys.stdin)
d['fe_pla_aniomes_desde'] = '202606'
d['status'] = 'RUNNING'
d['completed_at'] = ''
d['pendientes_antes'] = ''
d['pendientes_despues'] = ''
d['last_generados'] = ''
d['last_actualizados'] = ''
d['last_errores'] = ''
d['last_error'] = ''
d['detalle'] = 'Cambio manual a 202606'
d['updated_at'] = '$(date '+%Y-%m-%d %H:%M:%S')'
json.dump(d, open('logs/cobertura_auto_resume_state.json','w'), indent=2, ensure_ascii=False)
"

# Relanzar
bash scripts/run_resume_coberturas.sh
```

## Procesar un solo trámite urgente (sin worker)

```bash
cd /data_nuevo/cobertura_integrada
python3 -c "
import os; os.environ['TZ']='America/Bogota'
from src.auto_resume_state import registrar_job_activo
registrar_job_activo(fe_pla_aniomes_desde='202605', output_dir='/data_nuevo/coberturas', dig_tramite='5959391')
"
bash scripts/run_resume_coberturas.sh
```

## Limpiar locks huérfanos

```bash
rm -f /tmp/cobertura_auto.lock
rm -f /data_nuevo/cobertura_integrada/config/stop_cobertura.flag
```

## Liberar cuarentena manualmente

```bash
sqlite3 /data_nuevo/cobertura_integrada/logs/cobertura_quarantine.sqlite "DELETE FROM quarantine;"
```

## Forzar corrida ignorando el intervalo de espera

Si el worker no arranca porque `job_debe_reanudarse()` devuelve False por el intervalo de espera, forzalo así:

```bash
cd /data_nuevo/cobertura_integrada
python3 -c "
import json
d = json.load(open('logs/cobertura_auto_resume_state.json'))
d['updated_at'] = '2000-01-01 00:00:00'
json.dump(d, open('logs/cobertura_auto_resume_state.json','w'), indent=2)
"
bash scripts/run_resume_coberturas.sh
```

## Ver el log en vivo mientras corre

```bash
tail -f $(ls -t /data_nuevo/cobertura_integrada/logs/cobertura_auto_*.jsonl | head -1)
```
