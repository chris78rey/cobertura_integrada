# Cobertura Integrada MSP

Generación automática de hojas de cobertura de salud desde Oracle MSP, con login automático, progreso en tiempo real, ritmo dinámico y observabilidad completa.

## Arquitectura

```
┌─────────────────────────────────────────────────────┐
│  Streamlit (Python)  ──── puerto 8502               │
│  app.py → auth.py → dashboard.py                    │
│             → cobertura_pdf.py (consulta Oracle)    │
│             → observability.py (logs JSONL)         │
├─────────────────────────────────────────────────────┤
│  JDBC (jaydebeapi + ojdbc8.jar)                     │
│  └── Oracle RAC 11g: 172.16.60.21:1521:PRDSGH2     │
├─────────────────────────────────────────────────────┤
│  Node.js (proyecto externo)                         │
│  └── scripts/generate_pdf.js → PDF + SVG → solo PDF│
└─────────────────────────────────────────────────────┘
```

## Ramas

| Rama | Propósito |
|------|-----------|
| `main` | Versión original con login manual y exportación por ID_GENERACION |
| `CORRE_AUTOMATIZADO` | **Activa.** Modo automático sin login, consulta masiva desde FE_PLA_ANIOMES >= 202604 |

## Setup rápido

```bash
# 1. Clonar
git clone https://github.com/chris78rey/cobertura_integrada.git
cd cobertura_integrada

# 2. Rama correcta
git checkout CORRE_AUTOMATIZADO

# 3. Entorno virtual
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 4. .env (copiar y ajustar)
cp .env.example .env
nano .env

# 5. Lanzar
streamlit run app.py
# → http://localhost:8502
```

## Configuración (`.env`)

| Variable | Obligatorio | Descripción |
|----------|-------------|-------------|
| `ORACLE_TARGETS` | ✅ | host:puerto:sid (failover separado por coma) |
| `ORACLE_JDBC_JAR` | ✅ | Ruta absoluta al driver JDBC |
| `ORACLE_AUTO_USER` | ✅ | Usuario Oracle (ej: DIGITALIZACION) |
| `ORACLE_AUTO_PASSWORD` | ✅ | Contraseña Oracle |
| `AUTO_FE_PLA_ANIOMES_DESDE` | ✅ | Mes desde el que procesar (ej: 202604) |
| `COBERTURA_NODE_PROJECT_DIR` | ✅ | Carpeta del proyecto Node con scripts/generate_pdf.js |
| `COBERTURA_OUTPUT_DIR` | ✅ | Carpeta donde guardar PDFs y manifiestos |
| `COBERTURA_NODE_BIN` | ❌ | Binario node (default: node) |

## Estructura del proyecto

```
cobertura_integrada/
├── app.py                    # Entry point Streamlit (login automático)
├── .env                      # Config local (no subir a git)
├── .env.example              # Template de configuración
├── requirements.txt
├── jdbc/
│   └── ojdbc8.jar            # Driver Oracle JDBC (7.2 MB)
├── config/
│   ├── app_config.json       # Ruta de salida configurable desde pantalla
│   └── stop_cobertura.flag   # Bandera para detener el proceso
├── logs/                     # JSONL de observabilidad (ignorado por git)
├── src/
│   ├── oracle_jdbc.py        # Conexión Oracle con failover + UPDATE DIG_COBERTURA
│   ├── config.py             # Lectura de .env
│   ├── auth.py               # Login automático
│   ├── ui.py                 # Componentes CSS
│   ├── async_jobs.py         # Ejecución en segundo plano
│   ├── cobertura_pdf.py      # ★ Núcleo: generar PDFs + ritmo dinámico
│   ├── observability.py      # RunLogger con eventos JSONL
│   ├── app_config.py         # Persistencia de ruta de salida
│   └── pages/
│       └── dashboard.py      # Interfaz: config + generar + parar + logs
└── (proyecto Node externo)
    └── scripts/generate_pdf.js   # Generador PDF (vía subprocess)
```

## Flujo de operación

```text
1. Abrir http://localhost:8502
2. Verificar ruta de salida (editable)
3. Presionar "Generar coberturas automáticas"
4. El sistema:
   a. Consulta Oracle: FE_PLA_ANIOMES >= X, COBERTURA='N', PLANILLADO='S'
   b. Por cada trámite:
      - Crea carpeta /{output_dir}/{tramite}/
      - Genera CC.pdf (1 cobertura) o CC_01.pdf, CC_02.pdf (varias)
      - Verifica que el PDF exista y pese > 0 bytes
      - Actualiza DIG_COBERTURA='S' solo si todos los PDFs existen
      - Espera según carga del sistema (2-30s dinámico)
   c. Al terminar: manifiesto CSV + logs JSONL descargables
```

## Observabilidad

Por cada corrida se genera en `logs/`:

- `cobertura_auto_YYYYMMDD_HHMMSS.jsonl` — todos los eventos
- `cobertura_auto_YYYYMMDD_HHMMSS_errors.jsonl` — solo errores

Eventos registrados: `RUN_START`, `DB_QUERY_FINISHED`, `ROW_START`, `PDF_GENERATION_START`, `PDF_GENERATION_END`, `ORACLE_UPDATE_START`, `ORACLE_UPDATE_END`, `THROTTLE_WAIT`, `STOP_REQUESTED`, `RUN_END`

## Ritmo dinámico

El sistema mide CPU, RAM y disco con psutil:

| Estado | Espera | Condición |
|--------|--------|-----------|
| Normal | 2-4s | CPU < 75%, RAM < 80% |
| Media | 5-8s | CPU ≥ 75%, RAM ≥ 80%, PDF lento o 3+ errores |
| Alta | 10-20s | CPU ≥ 90%, RAM ≥ 90%, o 5+ errores |
| Crítica | 20-30s | Disco ≥ 95% |

## Reglas críticas

- **Solo se actualiza `DIG_COBERTURA='S'` si el PDF existe físicamente y pesa > 0 bytes**
- **No se modifica `DIG_PLANILLADO`**
- **Los PDFs se nombran `CC.pdf` (1 cobertura) o `CC_01.pdf`, `CC_02.pdf` (varias)**
- **Nunca usar cédula ni fecha en el nombre del PDF**

## Reversión

```bash
# Volver a main
git checkout main
git branch -D CORRE_AUTOMATIZADO

# Si Oracle quedó marcado mal
# (requiere backup previo BK_COBERTURA_AUTO_202604)
UPDATE DIGITALIZACION.DIGITALIZACION d
SET d.DIG_COBERTURA = 'N'
WHERE EXISTS (
    SELECT 1 FROM DIGITALIZACION.BK_COBERTURA_AUTO_202604 b
    WHERE b.DIG_ID_TRAMITE = d.DIG_ID_TRAMITE
);
COMMIT;
```
