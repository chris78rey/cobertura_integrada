# Graph Report - cobertura_integrada  (2026-11-15)

## Corpus Check
- 32 files · ~47,260 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 969 nodes · 1978 edges · 50 communities (47 shown, 3 thin omitted)
- Extraction: 96% EXTRACTED · 4% INFERRED · 0% AMBIGUOUS · INFERRED: 79 edges (avg confidence: 0.83)
- Token cost: 192,438 input · 0 output

## Community Hubs (Navigation)
- Generación de PDFs de cobertura
- Auditoría menores y cédulas
- Skills y lecciones operativas
- Watchdog y scripts auxiliares
- Estado y reanudación del worker
- Cuarentena y herramientas operador
- Sync al repositorio (script)
- Dashboard: monitor y cola
- Manual operativo
- Reglas de la skill de lecciones
- Configuración Repomix
- coberturactl y cambios seguros
- Ayuda memoria
- Generador PDF Node
- Conexión Oracle JDBC
- Informe DTIC
- Sync rápido por trámite
- Lecciones aprendidas (doc)
- Consulta en vivo al portal
- Guía para levantar servicios
- Corrección de cobertura X
- App Streamlit y UI
- Observabilidad y RunLogger
- Contexto y reglas de seguridad
- Plantilla SVG
- Trabajos asíncronos
- Servicios systemd y estados
- Skill de logs
- Skill de control del worker
- Configuración de ejecución Node
- Dashboard: página principal
- README del proyecto
- Configuración de la app
- Skill de seguridad
- Fallos frecuentes y mitigaciones
- Dependencias Node
- Skill de sync al repo
- Skill de autonomía
- Skill de operación
- Arquitectura y ritmo dinámico
- Skill de consultas Oracle
- Autenticación
- Logos en blanco del pie
- Causas de fallo
- Skill de revisión diaria
- Reglas de observabilidad
- Logos del encabezado
- Criterio de éxito

## God Nodes (most connected - your core abstractions)
1. `generar_coberturas_automaticas_desde_mes()` - 42 edges
2. `oracle_connect()` - 35 edges
3. `Lecciones aprendidas - Cobertura MSP` - 35 edges
4. `Skill: Lecciones aprendidas - Cobertura automatizada` - 34 edges
5. `Informe de funcionamiento de la plataforma de coberturas automáticas` - 22 edges
6. `dashboard_page()` - 20 edges
7. `guardar_estado_job()` - 18 edges
8. `🏥 Cobertura Automática MSP — Manual Operativo` - 18 edges
9. `Skill cobertura-contexto` - 18 edges
10. `_regenerar_local_y_reemplazar_cc()` - 17 edges

## Surprising Connections (you probably didn't know these)
- `Forzar DIG_COBERTURA='S' cuando PDFs ya existen` --semantically_similar_to--> `actualizar_cobertura_por_tramite_valor()`  [INFERRED] [semantically similar]
  .pi/skills/cobertura-oracle/SKILL.md → src/oracle_jdbc.py
- `Consulta de pendientes reales en Oracle (FE_PLA_ANIOMES >= mes, DIG_COBERTURA='N', DIG_PLANILLADO='S')` --calls--> `oracle_connect()`  [EXTRACTED]
  .pi/skills/cobertura-operacion/SKILL.md → src/oracle_jdbc.py
- `Forzar corrida ignorando intervalo de espera (updated_at=2000-01-01)` --references--> `job_debe_reanudarse()`  [INFERRED]
  .pi/skills/cobertura-worker-control/SKILL.md → src/auto_resume_state.py
- `Forzar corrida ignorando el intervalo de espera` --references--> `job_debe_reanudarse()`  [INFERRED]
  .pi/skills/cobertura-worker-control/SKILL.md → src/auto_resume_state.py
- `Re-lectura de DIG_PLANILLADO antes de archivar/generar PDFs (64c97d4)` --references--> `_leer_planillado_actual()`  [INFERRED]
  graphify-out/memory/query_20261114_221924_8acf220c_por_qué_trámites_del_mes_anterior_fallan_con_erro.md → src/cobertura_pdf.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Worker resume state lifecycle (state JSON, state module, worker, manual overrides)** — _pi_skills_cobertura_logs_skill_cobertura_auto_resume_state_json, src_auto_resume_state, scripts_resume_coberturas_auto, _pi_skills_cobertura_worker_control_skill_forzar_corrida, _pi_skills_cobertura_worker_control_skill_cambio_mes_base, _pi_skills_cobertura_autonomia_skill_estados_worker [INFERRED 0.85]
- **Official coverage pipeline: Oracle query -> PDF generation -> Oracle update -> repo sync** — _pi_skills_cobertura_contexto_skill_flujo_oficial, _pi_skills_cobertura_oracle_skill_tabla_digitalizacion, src_cobertura_pdf, src_oracle_jdbc, src_repo_sync, _pi_skills_cobertura_sync_repo_skill_reglas_sync [INFERRED 0.85]
- **DIG_PLANILLADO race window and mitigation** — graphify_out_memory_query_20261114_221924_8acf220c_por_qué_trámites_del_mes_anterior_fallan_con_erro_p_sin_planillar, _pi_skills_cobertura_oracle_skill_dig_planillado, graphify_out_memory_query_20261114_221924_8acf220c_por_qué_trámites_del_mes_anterior_fallan_con_erro_recheck_planillado, graphify_out_memory_query_20261114_221924_8acf220c_por_qué_trámites_del_mes_anterior_fallan_con_erro_omitido_no_planillado, src_cobertura_pdf_generar_coberturas_automaticas_desde_mes [INFERRED 0.85]
- **Reglas que condicionan marcar DIG_COBERTURA='S'** — lecciones_aprendidas_criterio_exito, skills_lecciones_aprendidas_cobertura_skill_regla_actualizacion_segura, readme_reglas_criticas, lecciones_aprendidas_destino_oficial_no_se_inventa, skills_lecciones_aprendidas_cobertura_skill_nombres_pdf_cc [INFERRED 0.85]
- **Servicios systemd de operacion de coberturas** — guia_levantar_servicios_cobertura_streamlit_service, guia_levantar_servicios_cobertura_auto_resume_service, guia_levantar_servicios_cobertura_progress_watchdog_timer [EXTRACTED 1.00]
- **Patron: revalidar dato justo antes de una accion costosa** — lecciones_aprendidas_omitido_no_planillado, lecciones_aprendidas_cache_destinos_obsoleta, lecciones_aprendidas_watchdog_inactividad, lecciones_aprendidas_verificar_hipotesis [INFERRED 0.75]
- **PDF header branding (MSP, RPIS, Escudo Ecuador)** — assets_logomsp, assets_logorpis, assets_escudo_ec [INFERRED 0.85]
- **PDF footer logo strip of RPIS member institutions (MSP, MinInterior, MinDefensa, IESS, ISSFA, ISSPOL)** — assets_logomsp, assets_mininterior, assets_mindefensa, assets_iess, assets_issfa, assets_isspol [INFERRED 0.85]

## Communities (50 total, 3 thin omitted)

### Community 0 - "Generación de PDFs de cobertura"
Cohesion: 0.06
Nodes (58): _buscar_destino_existente_tramite(), _copiar_cc_a_destino_existente(), _es_pdf_cc(), _escribir_marca_local(), _escribir_nota_local(), _exp_output_root(), _leer_tramite_especifico(), _leer_tramites_ayer() (+50 more)

### Community 1 - "Auditoría menores y cédulas"
Cohesion: 0.07
Nodes (46): _nombre_cc_por_secuencia(), _parse_fecha_yyyy_mm_dd(), ArchivoLock, ejecutar_coberturas_con_lock(), ProcesoCoberturaYaEnEjecucion, _ahora_id(), _ahora_iso(), _auditar_evento() (+38 more)

### Community 2 - "Skills y lecciones operativas"
Cohesion: 0.05
Nodes (45): Skill cobertura-autonomia, cobertura-auto-resume.timer, Worker states (RUNNING, RETRY_PENDING, RETRY_PENDING_SLOW, WAITING_OTHER_PROCESS, COMPLETED, STOPPED_BY_USER, PAUSED_BY_OPERATOR), Riesgos de autonomía (sin backoff, cuarentena eterna, RUNNING falso, locks huérfanos, logs sin límite), Skill cobertura-logs, Categorías de error (503 Service Unavailable, Timeout, Otro), cedulas_tramite_audit.jsonl (auditoría de corrección de cédulas), cobertura_auto_YYYYMMDD_HHMMSS.jsonl (eventos del worker) (+37 more)

### Community 3 - "Watchdog y scripts auxiliares"
Cohesion: 0.09
Nodes (30): Lesson: watchdog marcaba el worker como trabado estando sano, Answer, Outcome, Q: ¿Por qué el watchdog marcaba el worker como trabado (RESTART por 900+ min) estando sano?, Evento RESTART_FAILED (usuario sin permiso de systemctl restart), Source Nodes, Bug métrica de inactividad del watchdog (max de log, progreso, edad del proceso), build_parser() (+22 more)

### Community 4 - "Estado y reanudación del worker"
Cohesion: 0.12
Nodes (34): Forzar corrida ignorando el intervalo de espera, contar_pendientes(), _dias_busqueda_auto(), _es_modo_vigilante(), _limpiar_logs_antiguos(), log(), main(), _marcar_tramite_x() (+26 more)

### Community 5 - "Cuarentena y herramientas operador"
Cohesion: 0.11
Nodes (23): destrabar_para_reintento(), exportar_estado_json(), habilitar_reintento_automatico(), leer_estado_operador(), _leer_texto_seguro(), leer_ultimos_errores(), limpiar_bandera_parada(), limpiar_locks_huerfanos() (+15 more)

### Community 6 - "Sync al repositorio (script)"
Cohesion: 0.12
Nodes (21): Skill cobertura-contexto, Flujo oficial: Oracle -> PDF (Node+Playwright) -> /data_nuevo/coberturas/{TRAMITE} -> update Oracle -> sync repo, Cobertura Automática MSP (propósito del sistema), Rutas oficiales (/data_nuevo/coberturas, /data_nuevo/repo_grande/data/datos, logs, config), ahora(), ArchivoLock, buscar_destinos_por_tramite(), construir_indice_destinos() (+13 more)

### Community 7 - "Dashboard: monitor y cola"
Cohesion: 0.09
Nodes (17): obtener_tramites_en_cola(), _diagnostico_simple_operativo(), _disparar_worker_inmediato(), _ejecutar_despacho_inmediato(), _estado_semaforo(), _formatear_texto(), _formatear_ts_visible(), _leer_json_seguro() (+9 more)

### Community 8 - "Manual operativo"
Cohesion: 0.08
Nodes (25): 🏥 Cobertura Automática MSP — Manual Operativo, Comando de control coberturactl, 🕹️ Comando único: `coberturactl`, ⚙️ Configuración (.env), 📊 Consultar Oracle directamente, 📁 Dónde está cada cosa, Entrar a la app web, Estado de reanudacion cobertura_auto_resume_state.json (+17 more)

### Community 9 - "Reglas de la skill de lecciones"
Cohesion: 0.09
Nodes (22): 10. Observabilidad obligatoria, 11. Orden correcto del logger, 12. UI/UX obligatoria, 13. Regla de no rotura, 14. Checklist rápido antes de entregar cambios, 15. Rollback de emergencia, 1. Regla principal de negocio, 2. Regla de actualización segura (+14 more)

### Community 10 - "Configuración Repomix"
Cohesion: 0.07
Nodes (27): includeDiffs, includeLogs, includeLogsCount, sortByChanges, sortByChangesMaxCommits, ignore, customPatterns, useDefaultPatterns (+19 more)

### Community 11 - "coberturactl y cambios seguros"
Cohesion: 0.17
Nodes (9): Skill cobertura-cambios-seguros, Plan de rollback desde backups, Validación técnica con py_compile, Archivos de alto riesgo, Cobertura Automática MSP — Cambios seguros, Preparación obligatoria, Pruebas de regresión obligatorias, Rollback (+1 more)

### Community 12 - "Ayuda memoria"
Cohesion: 0.08
Nodes (25): 10. Flujo típico de trabajo, 1. Arquitectura, 2. Cómo entrar a la app, 3. Uso diario — Generar coberturas, 4. Sincronizar PDFs al repositorio oficial, 5. Si la app se cae o no responde, 6. Consultar Oracle directamente, 7. Revisar resultados del sync (+17 more)

### Community 13 - "Generador PDF Node"
Cohesion: 0.16
Nodes (24): buildFooterImages(), buildSvgPage(), ensureDir(), escapeXml(), estimateRowHeight(), formatCoverageDate(), fs, generatePdfFromResult() (+16 more)

### Community 14 - "Conexión Oracle JDBC"
Cohesion: 0.11
Nodes (12): jaydebeapi, JPype1, pandas, psutil, python-dotenv, streamlit, main(), get_jdbc_jar() (+4 more)

### Community 15 - "Informe DTIC"
Cohesion: 0.10
Nodes (19): Reproceso de tramite X -> N desde pantalla, 10. Comunicación con auditores y secretarías, 11. Riesgo de no reportar incidencias, 12. Mensaje clave para DTIC, 13. Mensaje clave para usuarios operativos, 14. Conclusión, 15. Recomendación final, 1. Objetivo (+11 more)

### Community 16 - "Sync rápido por trámite"
Cohesion: 0.17
Nodes (12): cobertura_repo_sync.sqlite (tracking de sync), cobertura_sync_queue.sqlite (cola de sync pendiente), Skill cobertura-sync-repo, DESTINO_AMBIGUO (múltiples carpetas con mismo trámite), Estados de sync (COPIADO, OMITIDO_YA_EXISTE_IDENTICO, OMITIDO_YA_EXISTE_DIFERENTE, DESTINO_NO_ENCONTRADO, DESTINO_AMBIGUO, SIN_PDFS_CC, ERROR_COPIA, ERROR_HASH), Lesson: DESTINO_AMBIGUO persiste tras retirar carpeta duplicada, Caché obsoleto del índice de destinos (_INDICE_DESTINOS_CACHE por proceso), _destinos_vigentes() (+4 more)

### Community 17 - "Lecciones aprendidas (doc)"
Cohesion: 0.10
Nodes (18): 10. Lección operativa de fondo, 11. Scripts experimentales, 12. Recordatorio corto, 13. Watchdog: el tiempo de vida del proceso no es inactividad (2026-11-14), 14. Oracle cambia `DIG_PLANILLADO` por debajo del worker (2026-11-14), 15. Los `X` no vuelven solos a `N` si el job 5407 está roto (2026-11-14), 16. Verificar la hipótesis antes de cambiar código (2026-11-14), 1. Regla principal del flujo (+10 more)

### Community 18 - "Consulta en vivo al portal"
Cohesion: 0.15
Nodes (17): crypto, CryptoJS, csconsulta(), extractCookie(), fs, https, loadCsv(), main() (+9 more)

### Community 19 - "Guía para levantar servicios"
Cohesion: 0.10
Nodes (19): 10. Casos especiales, 11. Cómo reprocesar un trámite, 12. Cómo corregir muchos `X`, 13. Dónde están los directorios principales, 14. Si algo se queda trabado, 15. Comandos rápidos útiles, 16. Resumen corto, 1. Qué hace esta aplicación (+11 more)

### Community 20 - "Corrección de cobertura X"
Cohesion: 0.14
Nodes (13): Skill cobertura-oracle, Consulta de un trámite específico (DIG_TRAMITE, DIG_CEDULA, DIG_MENOR_EDAD, dependientes), Forzar DIG_COBERTURA='S' cuando PDFs ya existen, Resumen global (planillados, generados, pendientes), Trámites con error (DIG_COBERTURA='N' pero con PDFs locales), reiniciar_fallos_tramite(), actualizar_cobertura_por_tramite(), actualizar_cobertura_por_tramite_valor() (+5 more)

### Community 21 - "App Streamlit y UI"
Cohesion: 0.21
Nodes (11): _auto_login(), main(), _ui_login_gate(), _ejecutar_sync_coberturas_repo(), badge_ok(), badge_warn(), card_close(), card_open() (+3 more)

### Community 22 - "Observabilidad y RunLogger"
Cohesion: 0.19
Nodes (4): append_cyclic_jsonl(), ensure_log_dir(), now_iso(), RunLogger

### Community 23 - "Contexto y reglas de seguridad"
Cohesion: 0.14
Nodes (10): Skill cobertura-seguridad, Riesgos de seguridad operativa (Streamlit expuesto, .env, sudo sin límite, logs sensibles), Roles: operador, supervisor, técnico, Cobertura Automática MSP — Contexto estable, Flujo oficial, Formato de toda respuesta técnica, Módulos importantes, Propósito (+2 more)

### Community 24 - "Plantilla SVG"
Cohesion: 0.22
Nodes (10): applyChromeToAllPages(), drawFooter(), drawHeader(), fs, loadSvgLayout(), parseHeaderLineY(), parseImageTransforms(), parseMatrix() (+2 more)

### Community 25 - "Trabajos asíncronos"
Cohesion: 0.21
Nodes (5): _elapsed_seconds(), get_executor(), render_current_job(), submit_job(), get_max_workers()

### Community 26 - "Servicios systemd y estados"
Cohesion: 0.23
Nodes (10): cobertura-auto-resume.service (worker continuo), cobertura-progress-watchdog.timer (vigilancia de progreso), Doble tanda por DIG_FECHA_PLANILLA < DIG_FECHA_ALTA, Guia para levantar servicios y usar la aplicacion de coberturas, Caso DIG_MENOR_EDAD=S (titular + dependientes, hasta 6 PDFs), Accion masiva X -> N (mes actual y anterior), Estados DIG_COBERTURA N / S / X, Job Oracle 5407 PR_CORRIGE_DIG_2_MESES (X -> N diario) (+2 more)

### Community 27 - "Skill de logs"
Cohesion: 0.17
Nodes (11): Auditoría de corrección de cédulas para un trámite, Buscar un trámite en todos los logs, Cobertura Automática MSP — Logs y diagnóstico, Conteo de errores por categoría, Errores de hoy, Estructura de logs, Filtrar solo eventos importantes, Ver cola de sync pendiente (+3 more)

### Community 28 - "Skill de control del worker"
Cohesion: 0.17
Nodes (11): Cambiar el mes base (ej: a 202606), Cobertura Automática MSP — Control directo del worker, Dejarlo corriendo en segundo plano, Lanzar el worker manualmente, Liberar cuarentena manualmente, Limpiar locks huérfanos, Matar el worker, Procesar un solo trámite urgente (sin worker) (+3 more)

### Community 29 - "Configuración de ejecución Node"
Cohesion: 0.27
Nodes (11): formatDateTimeInTimezone(), formatTimestampSlugInTimezone(), fs, getConfiguredTimezone(), getDateTimeParts(), getHttpsAgent(), https, isTrue() (+3 more)

### Community 30 - "Dashboard: página principal"
Cohesion: 0.17
Nodes (8): obtener_tramite_por_id(), dashboard_page(), _dias_busqueda_auto(), _init_state(), _render_auto_result(), _render_css(), _render_estado_worker(), _validar_fe_pla_aniomes_desde()

### Community 31 - "README del proyecto"
Cohesion: 0.18
Nodes (10): Cobertura Integrada MSP, Configuración (`.env`), Estructura del proyecto, Flujo de operación, Observabilidad, Ramas, Reglas críticas, Reversión (+2 more)

### Community 32 - "Configuración de la app"
Cohesion: 0.31
Nodes (7): _ensure_config_dir(), guardar_config(), leer_config(), _normalizar_config(), obtener_pdf_output_dir(), validar_directorio_salida(), _render_config_section()

### Community 33 - "Skill de seguridad"
Cohesion: 0.20
Nodes (9): Cobertura Automática MSP — Seguridad operativa, Operador, Reglas, Revisiones, Riesgos, Roles, Supervisor, Técnico (+1 more)

### Community 34 - "Fallos frecuentes y mitigaciones"
Cohesion: 0.22
Nodes (6): Diagnostico si el worker no avanza, Cedula invalida (no 10 digitos), Fallo DESTINO_AMBIGUO en sync, Fallo 503 Service Temporarily Unavailable del portal remoto, ERROR_ACTUALIZANDO_ORACLE: DIG_PLANILLADO=N, PCK_DIGITALIZACION.p_sin_planillar (UPDATE masivo DIG_PLANILLADO)

### Community 35 - "Dependencias Node"
Cohesion: 0.31
Nodes (8): dependencies, pdfkit@^0.17.2, playwright@^1.58.2, svg-to-pdfkit@^0.1.8, private, pdfkit, playwright, svg-to-pdfkit

### Community 36 - "Skill de sync al repo"
Cohesion: 0.22
Nodes (8): Cobertura Automática MSP — Sync al repositorio oficial, Diagnóstico, Diseño recomendado, Estados importantes, Objetivo, Pruebas después de cambios, Reglas, Riesgo principal

### Community 37 - "Skill de autonomía"
Cohesion: 0.25
Nodes (7): Cobertura Automática MSP — Autonomía, Componentes, Criterios para decir que corre solo, Estados esperados, Respuesta esperada, Revisión rápida, Riesgos

### Community 38 - "Skill de operación"
Cohesion: 0.25
Nodes (7): Cobertura Automática MSP — Operación diaria, Comando principal, Comandos disponibles, Diagnóstico inicial, Reglas de operación, Si el proceso no avanza, Ver pendientes reales en Oracle

### Community 39 - "Arquitectura y ritmo dinámico"
Cohesion: 0.29
Nodes (6): Arquitectura, Configuracion .env (ORACLE_TARGETS, AUTO_FE_PLA_ANIOMES_DESDE, ...), Rama CORRE_AUTOMATIZADO (modo automatico sin login), Cobertura Integrada MSP (README), Reversion con backup BK_COBERTURA_AUTO_202604, Rollback de emergencia (git checkout main)

### Community 40 - "Skill de consultas Oracle"
Cohesion: 0.29
Nodes (6): Cobertura Automática MSP — Consultas Oracle rápidas, Forzar DIG_COBERTURA='S' para un trámite (cuando PDFs ya existen), Pendientes por mes (202605 hacia adelante), Resumen global rápido, Top 10 trámites más recientes procesados, Un trámite específico

### Community 41 - "Autenticación"
Cohesion: 0.38
Nodes (4): clear_jobs_state(), login_screen(), logout_button(), test_login()

### Community 42 - "Logos en blanco del pie"
Cohesion: 0.40
Nodes (5): IESS logo (100x100, renders blank/white), ISSFA logo (100x100, renders blank/white), ISSPOL logo (100x100, renders blank/white), Ministerio de Defensa logo (236x97, renders blank/white), Ministerio del Interior logo (280x97, renders blank/white)

### Community 43 - "Causas de fallo"
Cohesion: 0.50
Nodes (4): 6.1 `503 Service Temporarily Unavailable`, 6.2 Cédula inválida, 6.3 PDF esperado no apareció, 6. Causas frecuentes de fallo

### Community 44 - "Skill de revisión diaria"
Cohesion: 0.50
Nodes (3): Cobertura Automática MSP — Revisión diaria, Si algo falla, Un solo comando para ver todo

### Community 45 - "Reglas de observabilidad"
Cohesion: 0.50
Nodes (3): Eventos JSONL de observabilidad por corrida, Observabilidad obligatoria con RUN_ID y logs JSONL, build_run_id()

### Community 46 - "Logos del encabezado"
Cohesion: 0.67
Nodes (3): Escudo de Armas del Ecuador (coat of arms, grayscale), Ministerio de Salud Publica wordmark logo, Red Publica Integral de Salud (RPIS) logo

## Ambiguous Edges - Review These
- `cobertura_runner.py` → `psutil`  [AMBIGUOUS]
  requirements.txt · relation: conceptually_related_to
- `Reglas que nunca deben romperse` → `Limpieza manual de locks y cuarentena (rm lock, DELETE FROM quarantine)`  [AMBIGUOUS]
  .pi/skills/cobertura-worker-control/SKILL.md · relation: conceptually_related_to

## Knowledge Gaps
- **25 isolated node(s):** `JAVA_TOOL_OPTIONS`, `JAVA_TOOL_OPTIONS`, `Riesgos de autonomía (sin backoff, cuarentena eterna, RUNNING falso, locks huérfanos, logs sin límite)`, `Rutas oficiales (/data_nuevo/coberturas, /data_nuevo/repo_grande/data/datos, logs, config)`, `Columna FE_PLA_ANIOMES (mes de planilla)` (+20 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 359 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `oracle_connect()` connect `Corrección de cobertura X` to `Generación de PDFs de cobertura`, `Auditoría menores y cédulas`, `Skills y lecciones operativas`, `Estado y reanudación del worker`, `Dashboard: monitor y cola`, `Manual operativo`, `Autenticación`, `Ayuda memoria`, `Conexión Oracle JDBC`, `Dashboard: página principal`?**
  _High betweenness centrality (0.190) - this node is a cross-community bridge._
- **What connects `JAVA_TOOL_OPTIONS`, `JAVA_TOOL_OPTIONS`, `Riesgos de autonomía (sin backoff, cuarentena eterna, RUNNING falso, locks huérfanos, logs sin límite)` to the rest of the system?**
  _25 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Generación de PDFs de cobertura` be split into smaller, more focused modules?**
  _Cohesion score 0.06224899598393574 - nodes in this community are weakly interconnected._
- **Why does `Flujo oficial: Oracle -> PDF (Node+Playwright) -> /data_nuevo/coberturas/{TRAMITE} -> update Oracle -> sync repo` connect `Sync al repositorio (script)` to `Generación de PDFs de cobertura`, `Generador PDF Node`?**
  _High betweenness centrality (0.137) - this node is a cross-community bridge._
- **Should `Auditoría menores y cédulas` be split into smaller, more focused modules?**
  _Cohesion score 0.06841046277665996 - nodes in this community are weakly interconnected._

### Low-confidence Hints
_AMBIGUOUS edges — the extractor was unsure. Verify before acting on these._

- **What is the exact relationship between `cobertura_runner.py` and `psutil`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `Reglas que nunca deben romperse` and `Limpieza manual de locks y cuarentena (rm lock, DELETE FROM quarantine)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._