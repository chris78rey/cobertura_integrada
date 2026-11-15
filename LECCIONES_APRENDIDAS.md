# Lecciones aprendidas - Cobertura MSP

Documento de memoria operativa y técnica del flujo de coberturas automáticas.

## 1. Regla principal del flujo

- El flujo toma solo trámites con `DIG_COBERTURA = 'N'`.
- Si está activo el modo de ventana por fecha, se filtra por `DIG_FECHA_HASTA` de los últimos días.
- Al terminar bien, el trámite pasa a `S`.
- Si falla tres veces, el trámite pasa a `X`.
- `X` significa revisión manual fuera del flujo automático.

## 2. El éxito no es solo generar el PDF

Un trámite solo se considera exitoso cuando se cumplen las tres condiciones:

1. El PDF local existe y pesa más de 0 bytes.
2. El PDF se copia al destino oficial del trámite.
3. Oracle verifica y actualiza `DIG_COBERTURA` a `S`.

Si falta uno de esos pasos, no se debe cerrar como éxito.

## 3. El destino oficial no se inventa

- El destino oficial es una carpeta existente en:
  - `/data_nuevo/repo_grande/data/datos/<AÑO>/<TIPO>/<DIG_TRAMITE>/`
- No se debe crear una carpeta nueva en destino si no existe.
- Solo se copian `CC*.pdf`.
- No se deben tocar otros PDFs del destino como `PI.pdf`, `08.pdf`, `010A_1.pdf`, etc.

## 4. La evidencia local se conserva

- Los PDFs locales no se deben borrar al éxito.
- Cada trámite debe dejar evidencia local de su resultado.
- Se usan marcas locales simples:
  - `PASO.txt` si el trámite salió bien.
  - `FALLO.txt` si el trámite falló.

Esto evita perder trazabilidad y permite revisar qué pasó sin depender del destino.

## 5. El flujo no debe mezclar corridas viejas con nuevas

- Si una carpeta local trae restos viejos, se puede confundir una corrida nueva.
- La solución segura es archivar o renombrar la carpeta anterior antes de reprocesar.
- El archivado por carpeta debe ser opcional, no obligatorio.

Regla de nombres recomendada para archivo:

- `DIG_TRAMITE_YYYYMMDD_HHMMSS`
- si choca, agregar `_2`, `_3`, etc.

## 6. Causas frecuentes de fallo

### 6.1 `503 Service Temporarily Unavailable`

- No es Oracle.
- No es la carpeta local.
- Es el portal o servicio remoto que no atendió la consulta.

Tratamiento:

- reintentar el trámite
- si sigue fallando, marcar `X`

### 6.2 Cédula inválida

- Si la cédula no tiene 10 dígitos, el PDF no debe intentar generarse como si fuera válida.
- Ese caso suele fallar en dependientes o datos mal cargados.

Tratamiento:

- no insistir indefinidamente
- dejar evidencia del error
- pasar a `X` si ya agotó los reintentos

### 6.3 PDF esperado no apareció

- A veces el generador termina, pero no deja el archivo esperado.
- En ese caso el trámite no debe cerrarse como éxito.

Tratamiento:

- registrar `FALLO.txt`
- conservar local
- reintentar hasta el límite

## 7. Reintentos

Regla aplicada:

- 1er fallo: sigue en `N`
- 2do fallo: sigue en `N`
- 3er fallo: pasa a `X`

Esto evita llenar Oracle de `X` por fallos transitorios.

## 8. Ventana de trabajo

La ventana operativa puede cambiarse por configuración:

- `AUTO_FECHA_HASTA_DIAS_ATRAS=5`

Con eso, el worker trabaja por los registros recientes según `DIG_FECHA_HASTA`.

Si esa variable no está activa, el flujo puede volver al corte por `FE_PLA_ANIOMES`.

## 9. Lo que ya no conviene volver a hacer

- No volver a crear el flujo completo de forma duplicada.
- No volver a depender de botones manuales para destrabar el proceso.
- No volver a borrar el origen como parte normal del éxito.
- No mezclar el flujo experimental con el flujo principal.
- No asumir que `S` se puede escribir si no hubo sync válido.

## 10. Lección operativa de fondo

El usuario debe poder dejar el sistema corriendo sin vigilarlo todo el tiempo.

Por eso, el proceso debe cumplir esto:

- leer Oracle
- generar local
- copiar al destino existente
- marcar `S` o `X`
- seguir con el siguiente

Si algo falla, se registra.
Si algo funciona, se conserva evidencia.

## 11. Scripts experimentales

Los scripts experimentales sirven para probar ideas sin tocar el flujo principal.

Uso:

- validar lógica
- revisar órdenes de procesamiento
- probar ventanas nuevas
- confirmar formato de PDF

No deben reemplazar al worker principal sin revisión.

## 12. Recordatorio corto

- `N` = pendiente
- `S` = terminado y sincronizado
- `X` = fallo manual / fuera del flujo automático
- local se conserva
- destino no se inventa
- `CC*.pdf` son los únicos PDFs que se copian

## 13. Watchdog: el tiempo de vida del proceso no es inactividad (2026-11-14)

**Error:** el watchdog calculaba la inactividad con `max(log, progreso, vida_del_proceso)`. Cualquier worker con más de 5 min de vida parecía "trabado". El log acumuló más de 8 000 `RESTART` falsos.

**Por qué no hizo daño:** el watchdog corre como `spd-he1` y llamaba a `systemctl restart` **sin `sudo`**, así que fallaba y descartaba el error. Ningún reinicio se ejecutó; el efecto real era que **no había watchdog**. El permiso sí existía: sudoers da a `spd-he1` `NOPASSWD: /usr/bin/systemctl *, /usr/bin/journalctl *` (lo usa el bot de Telegram).

**Corrección (commit 64c97d4):** inactividad = `min(...)` de señales de vida reales (journal, `last_progress_at`, `updated_at` del estado). Se revisa el código de salida de `systemctl` y se registra `RESTART_FAILED`.

**Segunda corrección (2026-11-14):** reinicio con `sudo -n /usr/bin/systemctl ...` y umbral de 10 min (hueco máximo real entre señales de vida ~99 s en 51 corridas; la consulta del lote a Oracle tiene timeout de 300 s).

**Regla:**
- Medir inactividad por la señal de vida **más reciente**, nunca por la más antigua ni por la edad del proceso.
- Toda acción correctiva (reinicio, UPDATE) debe revisar su resultado y dejarlo en el log.
- Probar cambios del watchdog con `COBERTURA_WATCHDOG_DRY_RUN=1`: el timer lo ejecuta cada minuto desde el repo, así que editarlo es desplegarlo.

## 14. Oracle cambia `DIG_PLANILLADO` por debajo del worker (2026-11-14)

**Síntoma:** trámites del mes anterior generaban PDFs y luego fallaban con `ERROR_ACTUALIZANDO_ORACLE: No se actualiza porque DIG_PLANILLADO=N`, corrida tras corrida.

**Causa raíz (fuera del repo):** `PCK_DIGITALIZACION.p_sin_planillar` hace `UPDATE digitalizacion SET dig_planillado = 'S'` en **toda la tabla**, `COMMIT`, y luego regresa a `N` fila por fila los que no tienen cuenta en `sis.cuentas`. El lote del worker captura esa ventana.

**Corrección (commit 64c97d4):** se relee `DIG_PLANILLADO` justo antes de archivar la carpeta o generar PDFs. Si ya no es `S`, el trámite queda `OMITIDO_NO_PLANILLADO`, sin generar PDFs y **sin contarlo como fallo** (no pasa a `X`).

**Regla:**
- Un dato leído en lote puede cambiar antes de usarlo: revalidar justo antes de una acción costosa o irreversible.
- Una omisión por dato externo no es un fallo del trámite; no debe sumar reintentos hacia `X`.
- Pendiente con el DBA: reescribir `p_sin_planillar` sin el UPDATE masivo.

## 15. Los `X` no vuelven solos a `N` si el job 5407 está roto (2026-11-14)

- El job Oracle 5407 `PR_CORRIGE_DIG_2_MESES` (diario 06:00) pasa `X` a `N` en el mes actual y el anterior.
- Estaba en `BROKEN = Y`, sin última ejecución. Por eso los `X` quedaban abandonados y hubo que resetearlos con un UPDATE manual.
- **Regla:** si se acumulan `X`, revisar primero `USER_JOBS` (`BROKEN`, `FAILURES`) antes de buscar el problema en el worker.
- Diagnóstico: un conteo de solo lectura por `FE_PLA_ANIOMES` y `DIG_COBERTURA` muestra en segundos si hay `X` acumulados.

## 16. Verificar la hipótesis antes de cambiar código (2026-11-14)

- El primer diagnóstico ("el worker estuvo trabado 15 horas") era falso: el worker estaba bien, en vigilancia, porque los pendientes estaban en `X`, no en `N`.
- El dato engañoso fue el propio log del watchdog (sección 13).
- **Regla:** antes de corregir, contrastar el síntoma con al menos dos fuentes independientes: journal del servicio, log JSONL de la corrida, `systemctl show` (`NRestarts`, `MainPID`) y Oracle.

## 17. Una caché en memoria de un proceso largo no ve los cambios del disco (2026-11-15)

**Síntoma:** después de retirar la carpeta vieja de 12 trámites (`EMR10` → cuarentena, lote #1 de "Duplicados por cambio de mes" del visor), el sync siguió fallando con `DESTINO_AMBIGUO` y los 12 volvieron a `X`.

**Causa:** `src/repo_sync.py` guarda el índice de carpetas destino en `_INDICE_DESTINOS_CACHE` una sola vez por proceso. El worker, vivo desde horas antes, seguía "viendo" la carpeta retirada. El mismo defecto escondía otro: tampoco veía las carpetas **nuevas** que el generador crea mientras el worker corre.

**Corrección:** `_destinos_vigentes()` revisa en disco, para el trámite que se va a copiar, las rutas de la caché (descarta las que ya no existen) y las `SERVICIO/<tramite>` del año (agrega las nuevas). Cuesta ~1 ms.

**Regla:**
- Toda caché de un proceso que vive horas necesita una forma de invalidarse o de revalidar el dato justo antes de usarlo.
- Cuando otro sistema mueve o retira carpetas, revisar quién las tiene en caché: aquí eran el worker de coberturas y los 2 procesos del visor (este último se resuelve con su reindexación).
- Al reintentar trámites en `X`, primero asegurarse de que la causa ya no está: si no, en pocos ciclos vuelven a `X`.

---

Última actualización: 2026-11-15
