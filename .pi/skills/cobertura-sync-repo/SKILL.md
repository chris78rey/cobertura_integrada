---
name: cobertura-sync-repo
description: Diagnostica y mejora el sync de PDFs hacia el repositorio oficial con miles de carpetas. Úsalo cuando se hable de sync, repo, 114000 carpetas, hashes, destino no encontrado, destino ambiguo, lentitud o I/O.
---

# Cobertura Automática MSP — Sync al repositorio oficial

## Objetivo

Copiar PDFs desde `/data_nuevo/coberturas/{TRAMITE}/` hacia `/data_nuevo/repo_grande/data/datos/.../{TRAMITE}/` sin sobrescribir y dejando trazabilidad.

## Reglas

1. No sobrescribir destino.
2. Validar SHA256.
3. Registrar CSV y SQLite.
4. Si hay destino ambiguo, no copiar.
5. Si no hay destino, registrar.
6. Sync completo no debe ejecutarse innecesariamente.

## Diagnóstico

```bash
time find /data_nuevo/repo_grande/data/datos -type d | wc -l
du -sh /data_nuevo/repo_grande/data/datos
du -sh /data_nuevo/coberturas
du -sh /data_nuevo/cobertura_integrada/logs
```

```bash
cd /data_nuevo/cobertura_integrada
sqlite3 logs/cobertura_repo_sync.sqlite "SELECT estado, COUNT(*) FROM cobertura_repo_sync GROUP BY estado ORDER BY COUNT(*) DESC;"
sqlite3 logs/cobertura_repo_sync.sqlite "SELECT tramite, archivo, estado, detalle, created_at FROM cobertura_repo_sync ORDER BY id DESC LIMIT 30;"
```

## Estados importantes

- `COPIADO` — copia exitosa
- `OMITIDO_YA_EXISTE_IDENTICO` — ya existe, mismo hash
- `OMITIDO_YA_EXISTE_DIFERENTE` — existe pero hash diferente (no se toca)
- `DESTINO_NO_ENCONTRADO` — no hay carpeta en el repo
- `DESTINO_AMBIGUO` — múltiples carpetas con mismo trámite
- `SIN_PDFS_CC` — carpeta origen sin PDFs
- `ERROR_COPIA` / `ERROR_HASH` — fallo técnico

## Riesgo principal

Con más de 100.000 carpetas, el sync completo frecuente puede causar carga fuerte de disco.

## Diseño recomendado

- Sync normal: incremental por trámites generados.
- Sync completo: nocturno o manual.
- Mantener cola local de trámites pendientes.
- Registrar SYNC_OK, SYNC_ERROR, DESTINO_NO_ENCONTRADO.

## Pruebas después de cambios

1. Copia trámite nuevo.
2. No sobrescribe PDF existente.
3. Detecta destino ambiguo.
4. Detecta destino no encontrado.
5. Registra SQLite y CSV.
6. No recorre todo el repo si solo hay cola incremental.
