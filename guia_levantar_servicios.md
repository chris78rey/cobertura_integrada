# Guía para levantar servicios y usar la aplicación de coberturas

## 1. Qué hace esta aplicación

Esta aplicación automatiza la generación de coberturas desde Oracle, crea los PDFs locales, sincroniza solo los `CC*.pdf` al repositorio destino y actualiza el estado del trámite en Oracle.

Estados principales:

- `N`: pendiente
- `S`: procesado y sincronizado correctamente
- `X`: falló varias veces o quedó para revisión manual

## 2. Servicios que se usan

La operación normal se apoya en estos componentes:

- `cobertura-streamlit.service`: interfaz web
- `cobertura-auto-resume.service`: worker automático continuo
- `cobertura-progress-watchdog.timer`: vigilancia de progreso

## 3. Cómo levantar los servicios

Ejecutar desde terminal con permisos de `sudo`:

```bash
sudo systemctl start cobertura-streamlit.service
sudo systemctl start cobertura-auto-resume.service
sudo systemctl start cobertura-progress-watchdog.timer
```

Si ya estaban arriba y quieres forzar recarga:

```bash
sudo systemctl restart cobertura-streamlit.service
sudo systemctl restart cobertura-auto-resume.service
sudo systemctl restart cobertura-progress-watchdog.timer
```

## 4. Cómo verificar que están activos

```bash
systemctl is-active cobertura-streamlit.service
systemctl is-active cobertura-auto-resume.service
systemctl is-active cobertura-progress-watchdog.timer
```

Ver detalle de estado:

```bash
systemctl status cobertura-streamlit.service --no-pager -l
systemctl status cobertura-auto-resume.service --no-pager -l
systemctl status cobertura-progress-watchdog.timer --no-pager -l
```

## 5. Cómo ver los logs

Interfaz web:

```bash
journalctl -u cobertura-streamlit.service -f
```

Worker automático:

```bash
journalctl -u cobertura-auto-resume.service -f
```

Watchdog:

```bash
journalctl -u cobertura-progress-watchdog.service -f
```

## 6. Cómo abrir la aplicación

La interfaz principal se abre en Streamlit.

Normalmente la ruta es:

- `http://localhost:8501`

Si el entorno usa otro puerto, revisar el servicio o el comando de arranque.

## 7. Qué hace la pantalla principal

La pantalla principal muestra:

- estado del worker
- pendientes actuales
- último trámite procesado
- si el proceso está activo o en espera
- acciones útiles para reproceso manual

En la vista simple, la intención es mostrar lo más importante sin recargar al operador.

## 8. Flujo operativo general

1. Oracle entrega trámites con `DIG_COBERTURA = 'N'`.
2. El worker toma los pendientes válidos.
3. Genera PDFs en local bajo `/data_nuevo/coberturas/<DIG_TRAMITE>/`.
4. Valida que el PDF o conjunto de PDFs exista realmente.
5. Sincroniza solo los `CC*.pdf` al destino existente del trámite.
6. Si todo sale bien, marca `S`.
7. Si falla repetidamente, marca `X`.

## 9. Reglas importantes del proceso

- Solo se procesan coberturas `N`.
- `X` no entra al worker automático.
- El destino no crea carpetas nuevas.
- Solo se copian `CC*.pdf`.
- Los demás PDFs del destino no se tocan.
- Los PDFs locales se conservan como respaldo.

## 10. Casos especiales

### Menor de edad

Si `DIG_MENOR_EDAD = 'S'`:

- se considera el titular
- se consideran los dependientes
- pueden generarse 3 PDFs por tanda

Si además hay una fecha de alta posterior a la fecha de planilla:

- se genera una tanda con `DIG_FECHA_PLANILLA`
- se genera otra con `DIG_FECHA_ALTA`
- en menores de edad eso puede dar 6 PDFs

### Fecha de alta

Si `DIG_FECHA_PLANILLA < DIG_FECHA_ALTA`:

- la primera tanda usa la fecha de planilla
- la segunda usa la fecha de alta

## 11. Cómo reprocesar un trámite

Desde la pantalla:

- buscar por `DIG_TRAMITE`
- revisar el estado actual
- si está en `X`, puede volver a `N` manualmente
- dejar que el worker lo vuelva a tomar

## 12. Cómo corregir muchos `X`

La pantalla tiene una acción masiva para cambiar `X` a `N` en:

- mes actual
- mes anterior

Eso sirve para reactivar trámites que se quieran volver a intentar sin tocar uno por uno.

## 13. Dónde están los directorios principales

Trabajo local:

- `/data_nuevo/coberturas`

Repositorio destino:

- `/data_nuevo/repo_grande/data/datos`

## 14. Si algo se queda trabado

Primero revisar:

1. `cobertura-auto-resume.service`
2. `cobertura-streamlit.service`
3. `cobertura-progress-watchdog.timer`
4. logs en `journalctl`

Si el worker no avanza:

- revisar si hay `X`
- revisar si el portal externo respondió `servicio no disponible`
- revisar si el destino existe para el trámite

## 15. Comandos rápidos útiles

```bash
sudo systemctl restart cobertura-auto-resume.service
sudo systemctl restart cobertura-streamlit.service
sudo systemctl restart cobertura-progress-watchdog.timer

systemctl is-active cobertura-auto-resume.service cobertura-streamlit.service

journalctl -u cobertura-auto-resume.service -f
```

## 16. Resumen corto

- `N` entra al flujo.
- `S` ya quedó bien.
- `X` queda fuera hasta reproceso.
- El worker corre solo.
- La UI solo muestra y permite corregir.
- El destino recibe solo `CC*.pdf`.

