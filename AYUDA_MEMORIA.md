# 🏥 Cobertura Automática MSP — Ayuda Memoria

> Servidor: `srvgestionred` (172.16.60.127)  
> Carpeta: `/data_nuevo/cobertura_integrada`  
> Acceso web: `http://172.16.60.127`

---

## 1. Arquitectura

```
┌─────────────────────────────────────────────────────────┐
│  Navegador (cualquier equipo de la red 172.16 / 192.168)│
│  http://172.16.60.127  (puerto 80)                      │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│  Nginx (puerto 80) → proxy reverso → Streamlit :8501    │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│  Streamlit (app.py)                                     │
│  ├── Autenticación automática contra Oracle              │
│  ├── Dashboard con filtros de mes/trámite               │
│  ├── Procesamiento por lotes dinámicos (100 x lote)     │
│  ├── Generación de PDFs vía Node.js + Playwright        │
│  └── UPDATE DIG_COBERTURA='S' en Oracle                 │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│  Oracle JDBC (172.16.60.21:1521:PRDSGH2)                │
│  Usuario: DIGITALIZACION                                │
└─────────────────────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│  PDFs generados en: /data_nuevo/coberturas/             │
│                      └── {tramite}/CC.pdf               │
│                      └── {tramite}/CC_01.pdf            │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│  sync_coberturas_repo.py                                │
│  Copia → /data_nuevo/repo_grande/data/datos/.../        │
│  Verifica SHA256, no sobrescribe, registra todo         │
└─────────────────────────────────────────────────────────┘
```

---

## 2. Cómo entrar a la app

Desde cualquier equipo de la red:

```
http://172.16.60.127
```

Si estás en subnet 192.168.x.x y no carga:
```
http://srvgestionred
```

---

## 3. Uso diario — Generar coberturas

1. Abrí `http://172.16.60.127`
2. Seleccioná modo: **"Procesar por mes desde"** o **"Procesar por trámite específico"**
3. Ingresá el mes (YYYYMM) y click en **"Generar coberturas automáticas"**
4. El proceso:
   - Muestra cuántos pendientes detecta
   - Procesa lotes de 100 en 100
   - Reconsulta Oracle por nuevos registros
   - Genera PDFs y actualiza `DIG_COBERTURA='S'`
5. Al terminar descarga el manifiesto CSV

### Botones

| Botón | Función |
|---|---|
| Generar coberturas automáticas | Inicia el proceso |
| Parar proceso | Detiene al finalizar la fila actual |
| Limpiar | Resetea pantalla y resultados |

---

## 4. Sincronizar PDFs al repositorio oficial

Después de generar coberturas, copiá los PDFs al repo:

```bash
cd /data_nuevo/cobertura_integrada

# Simular (ver qué haría sin copiar)
python3 scripts/sync_coberturas_repo.py --dry-run

# Copiar de verdad
python3 scripts/sync_coberturas_repo.py --apply
```

Esto copia todos los `CC*.pdf` desde `/data_nuevo/coberturas/` hacia `/data_nuevo/repo_grande/data/datos/.../`.

**Nunca sobrescribe** archivos que ya existen si el hash es diferente.

---

## 5. Si la app se cae o no responde

### Reiniciar Streamlit

```bash
cd /data_nuevo/cobertura_integrada
pkill -f "streamlit run"
source venv/bin/activate
export JAVA_TOOL_OPTIONS="-Doracle.jdbc.timezoneAsRegion=false"
streamlit run app.py --server.port 8501 --server.headless true &
```

### Ver si está corriendo

```bash
ps aux | grep streamlit
```

### Ver logs

```bash
# Log de Streamlit
cat /data_nuevo/cobertura_integrada/streamlit.log

# Log del proceso de coberturas (último)
tail -50 $(ls -t /data_nuevo/cobertura_integrada/logs/cobertura_auto_*.jsonl | head -1)
```

---

## 6. Consultar Oracle directamente

```bash
cd /data_nuevo/cobertura_integrada
source venv/bin/activate
export JAVA_TOOL_OPTIONS="-Doracle.jdbc.timezoneAsRegion=false"

python3 -c "
import os
from dotenv import load_dotenv
load_dotenv()
from src.oracle_jdbc import oracle_connect

user = os.environ['ORACLE_AUTO_USER']
password = os.environ['ORACLE_AUTO_PASSWORD']
conn = oracle_connect(user, password)

sql = '''
SELECT TRIM(TO_CHAR(FE_PLA_ANIOMES)) AS MES, COUNT(*) AS TOTAL
FROM DIGITALIZACION.DIGITALIZACION
WHERE TRIM(TO_CHAR(FE_PLA_ANIOMES)) >= '202604'
  AND NVL(TRIM(DIG_COBERTURA), 'N') = 'N'
  AND TRIM(DIG_PLANILLADO) = 'S'
GROUP BY TRIM(TO_CHAR(FE_PLA_ANIOMES))
ORDER BY 1
'''
stmt = conn.jconn.createStatement()
rs = stmt.executeQuery(sql)
while rs.next():
    print(rs.getString(1), rs.getInt(2))
conn.close()
"
```

---

## 7. Revisar resultados del sync

```bash
cd /data_nuevo/cobertura_integrada

# Ver resumen por estado
python3 -c "
import csv, collections, pathlib as p
f = sorted(p.Path('logs').glob('cobertura_repo_sync_apply_*.csv'))[-1]
c = collections.Counter()
with f.open() as fh:
    for r in csv.DictReader(fh): c[r['estado']] += 1
for e, t in c.most_common(): print(f'{e}: {t}')
"

# Ver todos los COPIADOS
python3 -c "
import csv, pathlib as p
f = sorted(p.Path('logs').glob('cobertura_repo_sync_apply_*.csv'))[-1]
with f.open() as fh:
    for r in csv.DictReader(fh):
        if r['estado'] == 'COPIADO': print(r['tramite'], r['archivo'], '→', r['destino'])
"
```

---

## 8. Configuración (.env)

Archivo: `/data_nuevo/cobertura_integrada/.env`

```ini
ORACLE_TARGETS=172.16.60.21:1521:PRDSGH2
ORACLE_JDBC_JAR=/data_nuevo/cobertura_integrada/jdbc/ojdbc8.jar
COBERTURA_NODE_PROJECT_DIR=/data_nuevo/cobertura_integrada
COBERTURA_OUTPUT_DIR=/data_nuevo/cobertura_integrada/coberturas_generadas
COBERTURA_NODE_BIN=/home/red_gestion/.nvm/versions/node/v20.20.1/bin/node
ORACLE_AUTO_USER=DIGITALIZACION
ORACLE_AUTO_PASSWORD=DIGITALIZACION
AUTO_FE_PLA_ANIOMES_DESDE=202605
```

Para cambiar el mes por defecto: editá `AUTO_FE_PLA_ANIOMES_DESDE`.

---

## 9. Comandos rápidos

```bash
# Reiniciar app
cd /data_nuevo/cobertura_integrada && pkill -f "streamlit run" && sleep 1 && source venv/bin/activate && export JAVA_TOOL_OPTIONS="-Doracle.jdbc.timezoneAsRegion=false" && streamlit run app.py --server.port 8501 --server.headless true &

# Sync a repo oficial
cd /data_nuevo/cobertura_integrada && python3 scripts/sync_coberturas_repo.py --apply

# Ver pendientes en Oracle
cd /data_nuevo/cobertura_integrada && source venv/bin/activate && export JAVA_TOOL_OPTIONS="-Doracle.jdbc.timezoneAsRegion=false" && python3 -c "import os; from dotenv import load_dotenv; load_dotenv(); from src.oracle_jdbc import oracle_connect; c=oracle_connect(os.environ['ORACLE_AUTO_USER'],os.environ['ORACLE_AUTO_PASSWORD']); s=c.jconn.createStatement(); r=s.executeQuery(\"SELECT COUNT(*) FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(TO_CHAR(FE_PLA_ANIOMES))>='202604' AND NVL(TRIM(DIG_COBERTURA),'N')='N' AND TRIM(DIG_PLANILLADO)='S'\"); r.next(); print('Pendientes:', r.getInt(1)); c.close()"
```

---

## 10. Flujo típico de trabajo

```
1. Entrar a http://172.16.60.127
2. Seleccionar "Procesar por mes desde"
3. Poner el mes (ej: 202605)
4. Click "Generar coberturas automáticas"
5. Esperar a que termine
6. Ejecutar: python3 scripts/sync_coberturas_repo.py --apply
7. Listo.
```

---

*Última actualización: 30 de abril 2026*
