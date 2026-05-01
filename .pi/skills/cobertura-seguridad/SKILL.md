---
name: cobertura-seguridad
description: Revisa exposición, permisos, secretos, Streamlit, .env, CORS/XSRF, rutas, sudo y roles de operador del proyecto Cobertura Automática MSP. Úsalo antes de exponer la app remotamente o revisar riesgo institucional.
---

# Cobertura Automática MSP — Seguridad operativa

## Riesgos

1. Streamlit expuesto sin control suficiente.
2. Credenciales en `.env`.
3. Operador con permisos excesivos.
4. Logs con datos sensibles.
5. Botones de destrabe accesibles sin rol.
6. Ruta crítica modificable.
7. Comandos sudo sin límite.

## Revisiones

```bash
cat /data_nuevo/cobertura_integrada/.streamlit/config.toml
ss -tulpn | egrep "80|8501|443"
sudo systemctl status nginx --no-pager
ls -l /data_nuevo/cobertura_integrada/.env
ls -ld /data_nuevo/cobertura_integrada
ls -ld /data_nuevo/coberturas
ls -ld /data_nuevo/repo_grande/data/datos
```

Proteger `.env`:
```bash
chmod 600 /data_nuevo/cobertura_integrada/.env
```

## Reglas

- No poner contraseñas reales en skills.
- No poner contraseñas reales en README/manuales.
- No compartir repomix con .env real.
- No exponer Streamlit directo a Internet.
- Usar VPN o red institucional.
- Separar roles: operador, supervisor, técnico.

## Roles

### Operador
- Ver estado, iniciar generación, parar controladamente, copiar diagnóstico.

### Supervisor
- Liberar cuarentena, limpiar locks huérfanos, ejecutar recover.

### Técnico
- Cambiar .env, cambiar systemd, cambiar código, hacer rollback, tocar Oracle.

## UI segura

- No permitir editar `/data_nuevo/coberturas`.
- No mostrar contraseñas.
- No permitir hard-reset desde pantalla.
- No permitir borrar PDFs.
- No permitir UPDATE manual a Oracle desde pantalla.
