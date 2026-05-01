# 🏥 Cobertura Automática MSP — Manual Operativo

> **Servidor:** `srvgestionred` (172.16.60.127)  
> **Carpeta:** `/data_nuevo/cobertura_integrada`  
> **Acceso web:** `http://172.16.60.127`  
> **Comando de control:** `sudo coberturactl`

---

## 🎯 ¿Qué hace este sistema?

Genera PDFs de cobertura desde Oracle y los sincroniza al repositorio oficial.

```
ORACLE (DIGITALIZACION.DIGITALIZACION)
    ↓ consulta pendientes (DIG_COBERTURA='N', DIG_PLANILLADO='S')
Node.js + Playwright genera PDFs CC.pdf, CC_01.pdf...
    ↓ guarda en /data_nuevo/coberturas/{TRAMITE}/
Oracle se actualiza: DIG_COBERTURA='S'
    ↓
Sync copia al repositorio oficial
    ↓ /data_nuevo/repo_grande/data/datos/.../TRAMITE/
```

---

## 🕹️ Comando único: `coberturactl`

```bash
sudo coberturactl COMANDO
```

| Comando | Qué hace |
|---|---|
| `status` | Muestra estado de todo: Streamlit, recuperador, locks, PDFs recientes |
| `start` | Inicia Streamlit y recuperador automático |
| `stop` | Parada controlada (no borra nada, solo detiene) |
| `restart` | Reinicia todo |
| `logs` | Últimos logs de Streamlit, recuperador y generación |
| `logs-follow` | Logs en vivo (Ctrl+C para salir) |
| `health` | Chequeo rápido: ¿está vivo? |
| `recover` | Ejecuta el recuperador una vez manualmente |
| `unlock` | Limpia locks huérfanos (solo si nadie los usa) |
| `hard-reset` | 🚨 Destraba todo: mata procesos colgados, limpia, reinicia |

---

## 📋 Flujo de trabajo diario

### Iniciar el sistema

```bash
sudo coberturactl start
```

### Entrar a la app web

```
http://172.16.60.127
```

### Generar coberturas

1. En la web, seleccioná **"Procesar por mes desde"**
2. Poné el mes (ej: `202605`)
3. Click en **"Generar coberturas automáticas"**
4. Esperá. La app:
   - Muestra pendientes detectados
   - Procesa lotes de 100
   - Reconsulta Oracle por nuevos registros
   - Genera PDFs y actualiza `DIG_COBERTURA='S'`
   - Al terminar, sincroniza automáticamente al repositorio oficial

### Ver estado en cualquier momento

```bash
sudo coberturactl status
```

### Si necesitás parar todo

```bash
sudo coberturactl stop
```

---

## 🚨 Solución de problemas

### "No se puede acceder al sitio"

```bash
sudo coberturactl status     # ¿está corriendo?
sudo coberturactl restart    # reiniciar
```

### "Ya existe un proceso de coberturas ejecutándose"

```bash
sudo coberturactl status     # ver quién tiene el lock
```

- Si aparecen **PDFs recientes**: el proceso está trabajando, **no tocar**.
- Si **no hay PDFs recientes** y el lock está tomado:

```bash
sudo coberturactl hard-reset  # mata proceso colgado y reinicia
```

### La app ve pendientes pero no procesa

```bash
sudo coberturactl logs        # revisar último error
sudo coberturactl recover     # ejecutar recuperador manualmente
```

### La bandera de parada quedó activa

```bash
sudo coberturactl start       # la limpia y arranca
```

---

## 📁 Dónde está cada cosa

| Recurso | Ruta |
|---|---|
| App principal | `/data_nuevo/cobertura_integrada/app.py` |
| Configuración | `/data_nuevo/cobertura_integrada/.env` |
| PDFs generados | `/data_nuevo/coberturas/{TRAMITE}/CC*.pdf` |
| Repositorio oficial | `/data_nuevo/repo_grande/data/datos/.../TRAMITE/` |
| Logs de generación | `/data_nuevo/cobertura_integrada/logs/cobertura_auto_*.jsonl` |
| Estado reanudación | `/data_nuevo/cobertura_integrada/logs/cobertura_auto_resume_state.json` |
| CSV de sync | `/data_nuevo/cobertura_integrada/logs/cobertura_repo_sync_*.csv` |
| SQLite sync | `/data_nuevo/cobertura_integrada/logs/cobertura_repo_sync.sqlite` |
| Lock generación | `/data_nuevo/cobertura_integrada/logs/cobertura_generation.lock` |
| Bandera de parada | `/data_nuevo/cobertura_integrada/config/stop_cobertura.flag` |

---

## ⚙️ Configuración (.env)

```ini
ORACLE_TARGETS=172.16.60.21:1521:PRDSGH2
ORACLE_AUTO_USER=DIGITALIZACION
ORACLE_AUTO_PASSWORD=DIGITALIZACION
AUTO_FE_PLA_ANIOMES_DESDE=202605      # ← mes por defecto en pantalla
COBERTURA_OUTPUT_DIR=/data_nuevo/cobertura_integrada/coberturas_generadas
COBERTURA_NODE_PROJECT_DIR=/data_nuevo/cobertura_integrada
COBERTURA_NODE_BIN=/home/red_gestion/.nvm/versions/node/v20.20.1/bin/node
```

---

## 🔄 Qué pasa si el servidor se reinicia

1. **systemd** levanta Streamlit automáticamente
2. **systemd timer** revisa cada 2 minutos si hay trabajo pendiente
3. Si encuentra estado `RUNNING` o `RETRY_PENDING`, reanuda solo
4. Como Oracle ya marcó lo procesado, continúa con lo pendiente

No tenés que hacer nada. El sistema se recupera solo.

---

## 📊 Consultar Oracle directamente

```bash
cd /data_nuevo/cobertura_integrada
source venv/bin/activate
export JAVA_TOOL_OPTIONS="-Doracle.jdbc.timezoneAsRegion=false"

python3 -c "
import os
from dotenv import load_dotenv
load_dotenv()
from src.oracle_jdbc import oracle_connect

c = oracle_connect(os.environ['ORACLE_AUTO_USER'], os.environ['ORACLE_AUTO_PASSWORD'])
s = c.jconn.createStatement()
r = s.executeQuery(\"SELECT TRIM(TO_CHAR(FE_PLA_ANIOMES)) MES, COUNT(*) TOTAL FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(TO_CHAR(FE_PLA_ANIOMES))>='202604' AND NVL(TRIM(DIG_COBERTURA),'N')='N' AND TRIM(DIG_PLANILLADO)='S' GROUP BY TRIM(TO_CHAR(FE_PLA_ANIOMES)) ORDER BY 1\")
while r.next(): print(r.getString(1), r.getInt(2))
c.close()
"
```

---

## 🧠 Recordá

- La app **nunca sobrescribe** PDFs en el repositorio oficial
- El sync se dispara **automáticamente** al terminar la generación
- Si hubo un corte, el recuperador **reanuda solo** cada 2 minutos
- `coberturactl hard-reset` es el último recurso, solo si nada más funciona
- Los PDFs generados nunca se borran automáticamente

---

*Última actualización: 1 mayo 2026*
