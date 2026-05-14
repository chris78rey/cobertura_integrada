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

---

Última actualización: 2026-05-14
