---
name: cobertura-oracle
description: Consultas Oracle rápidas para diagnosticar pendientes, trámites específicos, errores, avance por mes y estado de cobertura. Úsalo cuando necesites saber cuántos faltan, qué pasó con un trámite o cómo va el mes.
---

# Cobertura Automática MSP — Consultas Oracle rápidas

Todas se ejecutan sin salir del proyecto:

```bash
cd /data_nuevo/cobertura_integrada
```

## Pendientes por mes (202605 hacia adelante)

```bash
python3 -c "
import os; os.environ['TZ']='America/Bogota'
from src.oracle_jdbc import oracle_connect
c = oracle_connect('DIGITALIZACION','DIGITALIZACION')
s = c.jconn.createStatement()
r = s.executeQuery(\"SELECT TRIM(FE_PLA_ANIOMES) MES, COUNT(*) TOTAL, SUM(CASE WHEN TRIM(DIG_COBERTURA)='S' THEN 1 ELSE 0 END) GENERADOS FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(FE_PLA_ANIOMES) >= '202605' AND TRIM(DIG_PLANILLADO)='S' GROUP BY TRIM(FE_PLA_ANIOMES) ORDER BY 1\")
while r.next(): print(f'{r.getString(1)}: {r.getInt(3)}/{r.getInt(2)} (faltan {r.getInt(2)-r.getInt(3)})')
c.close()
"
```

## Un trámite específico

```bash
python3 -c "
import os; os.environ['TZ']='America/Bogota'
from src.oracle_jdbc import oracle_connect
c = oracle_connect('DIGITALIZACION','DIGITALIZACION')
ps = c.jconn.prepareStatement(\"SELECT DIG_TRAMITE, DIG_CEDULA, DIG_MENOR_EDAD, DIG_DEPENDIENTE_01, DIG_DEPENDIENTE_02, DIG_COBERTURA, DIG_PLANILLADO, FE_PLA_ANIOMES FROM DIGITALIZACION.DIGITALIZACION WHERE TO_CHAR(DIG_TRAMITE)=?\")
ps.setString(1, '5959391')
rs = ps.executeQuery()
if rs.next():
    print(f'Trámite: {rs.getString(1)} | Cédula: {rs.getString(2)} | Menor: {rs.getString(3)}')
    print(f'Dep1: {rs.getString(4)} | Dep2: {rs.getString(5)}')
    print(f'Cobertura: {rs.getString(6)} | Planillado: {rs.getString(7)} | Mes: {rs.getString(8)}')
else:
    print('No encontrado')
c.close()
"
```

## Resumen global rápido

```bash
python3 -c "
import os; os.environ['TZ']='America/Bogota'
from src.oracle_jdbc import oracle_connect
c = oracle_connect('DIGITALIZACION','DIGITALIZACION')
for q, label in [
    (\"SELECT COUNT(*) FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(DIG_PLANILLADO)='S'\", 'Total planillados'),
    (\"SELECT COUNT(*) FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(DIG_PLANILLADO)='S' AND TRIM(DIG_COBERTURA)='S'\", 'Generados'),
    (\"SELECT COUNT(*) FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(DIG_PLANILLADO)='S' AND NVL(TRIM(DIG_COBERTURA),'N')='N'\", 'Pendientes'),
]:
    s = c.jconn.createStatement()
    r = s.executeQuery(q)
    r.next()
    print(f'{label}: {r.getInt(1)}')
c.close()
"
```

## Trámites con error (DIG_COBERTURA='N' pero con PDFs locales)

```bash
python3 -c "
import os; os.environ['TZ']='America/Bogota'
from src.oracle_jdbc import oracle_connect
c = oracle_connect('DIGITALIZACION','DIGITALIZACION')
s = c.jconn.createStatement()
r = s.executeQuery(\"SELECT TO_CHAR(DIG_TRAMITE), DIG_CEDULA, FE_PLA_ANIOMES FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(DIG_PLANILLADO)='S' AND NVL(TRIM(DIG_COBERTURA),'N')='N' AND TRIM(FE_PLA_ANIOMES)='202605' AND ROWNUM <= 20\")
from pathlib import Path
while r.next():
    t = str(r.getString(1) or '').strip()
    tiene_pdf = Path(f'/data_nuevo/coberturas/{t}').exists()
    print(f'{t} | {r.getString(2)} | {r.getString(3)} | PDFs: {\"SI\" if tiene_pdf else \"NO\"}')
c.close()
"
```

## Forzar DIG_COBERTURA='S' para un trámite (cuando PDFs ya existen)

```bash
python3 -c "
import os; os.environ['TZ']='America/Bogota'
from src.oracle_jdbc import oracle_connect
c = oracle_connect('DIGITALIZACION','DIGITALIZACION')
c.jconn.setAutoCommit(False)
ps = c.jconn.prepareStatement(\"UPDATE DIGITALIZACION.DIGITALIZACION SET DIG_COBERTURA='S' WHERE TO_CHAR(DIG_TRAMITE)=? AND TRIM(DIG_PLANILLADO)='S'\")
ps.setString(1, '5958993')
a = ps.executeUpdate()
c.jconn.commit()
print(f'Actualizadas: {a} filas')
c.close()
"
```

## Top 10 trámites más recientes procesados

```bash
python3 -c "
import os; os.environ['TZ']='America/Bogota'
from src.oracle_jdbc import oracle_connect
c = oracle_connect('DIGITALIZACION','DIGITALIZACION')
s = c.jconn.createStatement()
r = s.executeQuery(\"SELECT TO_CHAR(DIG_TRAMITE), DIG_CEDULA, FE_PLA_ANIOMES, DIG_COBERTURA FROM DIGITALIZACION.DIGITALIZACION WHERE TRIM(DIG_PLANILLADO)='S' AND TRIM(FE_PLA_ANIOMES)='202605' AND DIG_TRAMITE IS NOT NULL ORDER BY DIG_TRAMITE DESC FETCH FIRST 10 ROWS ONLY\")
while r.next(): print(f'{r.getString(1)} | {r.getString(2)} | {r.getString(3)} | {r.getString(4)}')
c.close()
"
```
