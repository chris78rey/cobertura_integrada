---
name: lecciones-aprendidas-cobertura
description: Reglas, errores corregidos y decisiones técnicas aprendidas durante la implementación del proceso automático de generación de coberturas integradas PDF.
---

# Skill: Lecciones aprendidas - Cobertura automatizada

Esta skill debe usarse antes de modificar el flujo automático de generación de coberturas PDF del proyecto.

Aplica especialmente a la rama:

```text
CORRE_AUTOMATIZADO
```

y a los archivos:

```text
app.py
src/pages/dashboard.py
src/cobertura_pdf.py
src/oracle_jdbc.py
src/app_config.py
src/observability.py
config/app_config.json
```

---

## 1. Regla principal de negocio

El proceso automático debe consultar registros desde:

```sql
SELECT 
    DIG_TRAMITE,
    DIG_FECHA_HASTA,
    DIG_CEDULA,
    DIG_MENOR_EDAD,
    DIG_DEPENDIENTE_01,
    DIG_DEPENDIENTE_02,
    DIG_PLANILLADO,
    DIG_COBERTURA,
    DIG_ID_GENERACION,
    DIG_ID_TIPO,
    DIG_NUMERO_SOLICITUD,
    DIG_BLOQUEO_SGH,
    DIG_ID_TRAMITE,
    DIG_USUARIO
FROM DIGITALIZACION.DIGITALIZACION
WHERE FE_PLA_ANIOMES >= '202604'
  AND DIG_COBERTURA = 'N'
  AND DIG_PLANILLADO = 'S';
```

Después de generar correctamente el PDF de cobertura integrada, debe actualizarse únicamente:

```sql
DIG_COBERTURA = 'S'
```

No se debe actualizar:

```sql
DIG_PLANILLADO
```

Ese campo ya indica que el trámite está planillado.

---

## 2. Regla de actualización segura

Nunca se debe marcar una fila como generada si el PDF no existe.

El `UPDATE` correcto debe ser similar a:

```sql
UPDATE DIGITALIZACION.DIGITALIZACION
SET DIG_COBERTURA = 'S'
WHERE DIG_ID_TRAMITE = :dig_id_tramite
  AND DIG_COBERTURA = 'N'
  AND DIG_PLANILLADO = 'S';
```

La actualización debe ejecutarse solo después de validar:

```text
- El PDF existe.
- El PDF pesa más de 0 bytes.
- El nombre del PDF cumple la regla oficial.
```

---

## 3. Regla oficial de nombres PDF

No se debe usar cédula ni fecha en el nombre del PDF.

Incorrecto:

```text
CC_1714674288.pdf
CC_1714674288_20260401.pdf
CC_1723645907.pdf
```

Correcto cuando hay una sola cobertura en el trámite:

```text
CC.pdf
```

Correcto cuando hay varias coberturas en el mismo trámite:

```text
CC_01.pdf
CC_02.pdf
CC_03.pdf
```

Función obligatoria recomendada:

```python
def _nombre_cc_por_secuencia(indice: int, total: int) -> str:
    if total <= 1:
        return "CC"
    return f"CC_{indice:02d}"
```

El `output_name` enviado a `scripts/generate_pdf.js` debe salir únicamente de esta función.

---

## 4. Regla de agrupación

El proceso debe agrupar por trámite.

Ejemplo:

```text
5899568/
```

Si el trámite tiene titular y dos dependientes, debe quedar:

```text
5899568/
   ├── CC_01.pdf
   ├── CC_02.pdf
   └── CC_03.pdf
```

No debe quedar:

```text
5899568/
   ├── CC_1714674288.pdf
   ├── CC_1723645907.pdf
   └── CC_1761319456.pdf
```

---

## 5. Ruta configurable de salida

La ruta de salida no debe estar quemada en el código.

Ruta inicial por defecto:

```text
/home/crrb/coberturas_generadas/
```

Debe poder cambiarse desde pantalla y persistirse en:

```text
config/app_config.json
```

Ejemplo:

```json
{
  "pdf_output_dir": "/home/crrb/coberturas_generadas/"
}
```

Antes de iniciar el proceso, se debe validar:

```text
- Que la ruta sea absoluta.
- Que la carpeta exista o pueda crearse.
- Que tenga permisos de escritura.
```

---

## 6. Botón de parada controlada

Debe existir botón visible:

```text
Parar proceso
```

No debe matar el proceso de golpe.

Debe crear una bandera:

```text
config/stop_cobertura.flag
```

El proceso debe revisar esa bandera:

```text
- Antes de iniciar cada nueva fila.
- Durante la espera entre registros.
```

Si existe la bandera:

```text
- Termina la fila actual.
- Guarda manifiesto.
- No toma nuevas filas.
- Deja pendientes con DIG_COBERTURA='N'.
```

---

## 7. Pausa entre registros

Entre búsquedas/generaciones debe existir una pausa aleatoria base:

```text
2 a 4 segundos
```

Debe usarse como control de carga, no como evasión.

Ejemplo:

```python
espera = random.uniform(2, 4)
```

La espera debe ser interrumpible:

```python
inicio = time.monotonic()
while time.monotonic() - inicio < espera:
    if proceso_debe_parar():
        break
    time.sleep(0.5)
```

---

## 8. Ritmo dinámico por carga del sistema

La aplicación debe bajar el ritmo si detecta carga.

Indicadores:

```text
- CPU alta.
- RAM alta.
- Disco casi lleno.
- PDF tarda demasiado.
- Errores consecutivos.
```

Reglas sugeridas:

```text
Normal: 2 a 4 segundos.
Carga media: 5 a 8 segundos.
Carga alta: 10 a 20 segundos.
Disco crítico: 20 a 30 segundos o advertencia.
```

---

## 9. Variables que deben inicializarse siempre

Antes del ciclo principal:

```python
errores_consecutivos = 0
```

Dentro de cada iteración:

```python
ultimo_segundos_pdf = 0.0
```

Esto evita errores como:

```text
cannot access local variable 'errores_consecutivos' where it is not associated with a value
```

---

## 10. Observabilidad obligatoria

Cada corrida debe tener un identificador:

```text
RUN_ID
```

Ejemplo:

```text
cobertura_auto_20260429_184500
```

Debe generar logs:

```text
logs/<RUN_ID>.jsonl
logs/<RUN_ID>_errors.jsonl
```

Eventos mínimos:

```text
RUN_START
DB_QUERY_START
DB_QUERY_FINISHED
DB_QUERY_ERROR
ROW_START
PDF_GENERATION_START
PDF_GENERATION_END
ORACLE_UPDATE_START
ORACLE_UPDATE_END
THROTTLE_WAIT
STOP_REQUESTED
ROW_EXCEPTION
RUN_END
```

No se deben registrar contraseñas.

---

## 11. Orden correcto del logger

Nunca usar:

```python
logger.event(...)
```

antes de crear:

```python
logger = RunLogger(run_id)
```

Orden correcto:

```python
from src.observability import RunLogger, build_run_id
run_id = build_run_id("cobertura_auto")
logger = RunLogger(run_id)
logger.event("RUN_START")
```

Error aprendido:

```text
El proceso fallaba apenas se daba clic porque logger.event() se ejecutaba antes de crear logger.
```

---

## 12. UI/UX obligatoria

La pantalla debe mostrar de forma grande:

```text
125 / 17000
```

Debe mostrar:

```text
- Total procesado.
- Total general.
- Porcentaje.
- Barra de progreso grande.
- Generados.
- Errores.
- Pendientes.
- Tiempo transcurrido.
- Trámite actual.
- PDF actual.
- Estado actual.
```

Botones mínimos:

```text
Iniciar proceso
Parar proceso
Limpiar
```

La bitácora técnica debe ir en una sección colapsable.

---

## 13. Regla de no rotura

Antes de modificar el proceso automático, se debe verificar:

```text
- Que no se actualice DIG_COBERTURA antes de generar PDF.
- Que no se toque DIG_PLANILLADO.
- Que no se pierda el botón Parar proceso.
- Que no se pierda el manifiesto CSV.
- Que no se pierdan los logs JSONL.
- Que los nombres PDF no vuelvan a usar cédula ni fecha.
```

---

## 14. Checklist rápido antes de entregar cambios

```text
[ ] La app inicia sin login.
[ ] Usa usuario Oracle DIGITALIZACION desde variable de entorno.
[ ] Lee FE_PLA_ANIOMES >= '202604'.
[ ] Solo procesa DIG_COBERTURA='N'.
[ ] Solo procesa DIG_PLANILLADO='S'.
[ ] Genera CC.pdf si hay una cobertura.
[ ] Genera CC_01.pdf, CC_02.pdf si hay varias.
[ ] No genera CC_<CEDULA>.pdf.
[ ] No genera CC_<CEDULA>_<FECHA>.pdf.
[ ] Actualiza solo DIG_COBERTURA='S'.
[ ] No actualiza DIG_PLANILLADO.
[ ] Tiene botón Parar proceso visible.
[ ] Tiene pausa entre 2 y 4 segundos.
[ ] Tiene ritmo dinámico si hay carga.
[ ] Genera manifiesto CSV.
[ ] Genera logs JSONL.
[ ] Muestra contador grande en pantalla.
```

---

## 15. Rollback de emergencia

Si el cambio rompe el proceso:

```bash
git checkout main -- app.py src/pages/dashboard.py src/cobertura_pdf.py src/oracle_jdbc.py src/app_config.py src/observability.py
```

Si se necesita volver a la rama estable:

```bash
git checkout main
```

No se debe ejecutar ningún script de actualización Oracle si los PDFs no fueron validados.
