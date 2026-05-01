---
name: cobertura-operacion
description: Guía operativa para revisar estado, iniciar, detener, recuperar y diagnosticar Cobertura Automática MSP sin tocar código. Úsalo cuando el sistema no avanza, aparece lock, cuarentena, parada activa, RUNNING falso o problemas del worker.
---

# Cobertura Automática MSP — Operación diaria

## Comando principal

```bash
sudo coberturactl status
```

## Comandos disponibles

```bash
sudo coberturactl start
sudo coberturactl stop
sudo coberturactl restart
sudo coberturactl logs
sudo coberturactl logs-follow
sudo coberturactl health
sudo coberturactl recover
sudo coberturactl unlock
```

`hard-reset` solo se usa como emergencia.

## Diagnóstico inicial

```bash
cd /data_nuevo/cobertura_integrada
sudo coberturactl status
sudo systemctl status cobertura-streamlit.service --no-pager
sudo systemctl status cobertura-auto-resume.timer --no-pager
sudo systemctl list-timers | grep cobertura || true
```

## Si el proceso no avanza

```bash
cd /data_nuevo/cobertura_integrada
cat logs/cobertura_auto_resume_state.json 2>/dev/null || true

sqlite3 logs/cobertura_quarantine.sqlite "
SELECT clave, tramite, motivo, retry_count,
datetime(created_at,'unixepoch','localtime') creado,
datetime(expires_at,'unixepoch','localtime') expira
FROM quarantine WHERE expires_at > strftime('%s','now')
ORDER BY expires_at DESC LIMIT 30;
" 2>/dev/null || true

ps aux | egrep "streamlit|resume_coberturas|generate_pdf|node|playwright" | grep -v grep
```

## Ver pendientes reales en Oracle

Usar `202605` como base productiva actual, salvo instrucción contraria.

```bash
cd /data_nuevo/cobertura_integrada
source .venv/bin/activate 2>/dev/null || source venv/bin/activate
export JAVA_TOOL_OPTIONS="-Doracle.jdbc.timezoneAsRegion=false"

python3 - <<'PY'
import os
from dotenv import load_dotenv
load_dotenv()
from src.oracle_jdbc import oracle_connect
MES = "202605"
c = oracle_connect(os.environ["ORACLE_AUTO_USER"], os.environ["ORACLE_AUTO_PASSWORD"])
s = c.jconn.createStatement()
sql = f"SELECT TRIM(TO_CHAR(FE_PLA_ANIOMES)) MES, COUNT(*) TOTAL FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(TO_CHAR(FE_PLA_ANIOMES)) >= '{MES}' AND NVL(TRIM(DIG_COBERTURA),'N') = 'N' AND TRIM(DIG_PLANILLADO) = 'S' GROUP BY TRIM(TO_CHAR(FE_PLA_ANIOMES)) ORDER BY 1"
r = s.executeQuery(sql)
while r.next(): print(r.getString(1), r.getInt(2))
c.close()
PY
```

## Reglas de operación

* No borrar PDFs para destrabar.
* No cambiar Oracle manualmente sin respaldo.
* No liberar locks si hay proceso activo.
* No cambiar `/data_nuevo/coberturas`.
* No correr sync completo muchas veces seguidas.
* No usar 202604 como base de producción.
