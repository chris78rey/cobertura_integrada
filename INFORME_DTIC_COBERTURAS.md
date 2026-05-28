# Informe de funcionamiento de la plataforma de coberturas automáticas

## 1. Objetivo

El presente informe describe el funcionamiento general de la plataforma de coberturas automáticas, su propósito operativo, el rol de DTIC en su disponibilidad, y el manejo de incidencias cuando una cobertura se genera con contenido incorrecto, incompleto o con desconexiones temporales.

El objetivo principal del sistema es automatizar la generación de coberturas PDF y su envío al repositorio correspondiente, reduciendo la carga manual de descarga y carga de documentos.

---

## 2. Alcance

La plataforma cubre:

- Consulta automática de trámites pendientes en Oracle.
- Generación de PDFs de cobertura.
- Sincronización al repositorio oficial.
- Registro del estado de cada trámite.
- Reproceso automático o manual en caso de falla.
- Conservación de evidencia local para trazabilidad.

---

## 3. Rol de DTIC

DTIC es responsable de que la plataforma:

- permanezca activa,
- esté disponible para los usuarios,
- mantenga su ejecución operativa,
- y conserve la continuidad del servicio.

Esto incluye la supervisión técnica de la plataforma, pero no convierte a DTIC en responsable del contenido funcional de cada cobertura individual cuando la información de origen presenta inconsistencias temporales o requiere reproceso.

---

## 4. Funcionamiento general de la plataforma

La plataforma opera de forma automática siguiendo este flujo:

1. Consulta en Oracle los trámites pendientes.
2. Genera el PDF local de cobertura.
3. Verifica que el PDF se haya creado correctamente.
4. Copia el PDF al repositorio oficial.
5. Marca el trámite como finalizado en Oracle.
6. Si hay error, deja el trámite como pendiente o lo marca para revisión según la regla definida.

El sistema busca evitar que el usuario tenga que descargar y cargar manualmente los documentos.

---

## 5. Automatización vs operación manual

Antes del proceso automático, el trabajo dependía de descarga, revisión y carga manual.

Con la automatización:

- se elimina la descarga manual como paso rutinario,
- se reduce el error humano en la gestión de archivos,
- se acelera la disponibilidad de los documentos,
- se mantiene evidencia local y trazabilidad del proceso.

Sin embargo, la automatización no elimina la necesidad de control documental humano. Todavía es importante revisar casos excepcionales, validar resultados y coordinar reprocesos cuando exista una anomalía.

---

## 6. Sobre la revisión del estado de coberturas

La automatización no debe hacer que todos los usuarios tengan que revisar constantemente el estado de cada cobertura.

La idea es que:

- el sistema procese solo,
- el usuario solo intervenga cuando haya una excepción,
- y DTIC mantenga la plataforma operativa.

Lo que sí se requiere es un mecanismo de comunicación para casos anómalos, especialmente cuando:

- la cobertura se generó pero el contenido salió incorrecto,
- el PDF quedó inconsistente,
- hubo una desconexión temporal,
- o el documento llegó al repositorio con información que necesita reproceso.

---

## 7. Casos de error o desconexión

En algunos casos, el archivo puede llegar a generarse y subirse al repositorio, pero el contenido del PDF puede mostrar una desconexión, inconsistencia o resultado incorrecto.

Esto puede ocurrir por causas temporales relacionadas con:

- datos de origen,
- información de los financiadores del hospital,
- respuesta momentánea del sistema fuente,
- o una consulta que requiere repetirse.

Importante:

- Esto no necesariamente se origina en el HE1.
- Tampoco implica automáticamente una falla de red local.
- La solución suele ser reprocesar el trámite.

---

## 8. Solución operativa simple

Cuando se detecta una cobertura incorrecta o con desconexión temporal, la respuesta operativa adecuada es:

1. Informar la novedad.
2. Marcar el caso para reproceso.
3. Volver a ejecutar la generación.
4. Confirmar que el nuevo PDF salga correctamente.
5. Reemplazar o actualizar el documento según corresponda.

La ventaja de este enfoque es que el problema se resuelve rápidamente sin necesidad de una intervención compleja de infraestructura.

---

## 9. Control documental humano

Aunque la plataforma automatiza la generación, el control documental humano sigue siendo necesario para:

- identificar casos anómalos,
- verificar si un PDF subido requiere reproceso,
- comunicar observaciones a DTIC,
- coordinar con auditoría y secretaría,
- y asegurar que el documento final enviado sea correcto.

Esto no significa volver al modelo manual anterior. Significa que el proceso automatizado debe estar acompañado por una supervisión de excepción.

---

## 10. Comunicación con auditores y secretarías

Es importante establecer un flujo de comunicación claro con:

- personal de secretaría,
- auditores,
- y DTIC.

Ellos deben saber que:

- si una cobertura salió mal, no se debe asumir que el sistema quedó definitivamente fallido,
- la corrección suele ser sencilla,
- y el reproceso puede resolver el documento en poco tiempo.

Esto ayuda a evitar que un documento incorrecto se mantenga sin corrección y se termine remitiendo con inconsistencias al destino final.

---

## 11. Riesgo de no reportar incidencias

Si una novedad no se reporta:

- el archivo puede quedar subido con contenido incorrecto,
- puede llegar al repositorio o al receptor final sin corrección,
- y el caso quedaría documentado de forma inadecuada.

Por eso el control documental y la comunicación oportuna son parte esencial del flujo.

---

## 12. Mensaje clave para DTIC

Debe quedar claro que:

- DTIC es responsable de la disponibilidad y continuidad de la plataforma.
- La automatización reduce la necesidad de descarga manual.
- El sistema puede generar y subir documentos automáticamente.
- Cuando ocurre una desconexión o inconsistencia de contenido, normalmente se corrige con un reproceso.
- El problema no necesariamente proviene del HE1 ni de la red local.
- La solución operativa es sencilla y rápida si se reporta a tiempo.

---

## 13. Mensaje clave para usuarios operativos

- No es necesario revisar manualmente cada cobertura si el sistema está operando con normalidad.
- Solo deben reportarse las incidencias.
- Si un PDF sale con contenido inconsistente o desconectado, se reprocesa.
- La comunicación rápida evita que el documento incorrecto siga en circulación.

---

## 14. Conclusión

La plataforma de coberturas automáticas permite mejorar la operación, disminuir trabajo manual y acelerar la disponibilidad de documentos.

Su éxito depende de dos cosas:

1. La continuidad técnica garantizada por DTIC.
2. El control documental humano para gestionar excepciones y reprocesos.

La plataforma no reemplaza la supervisión documental; la hace más eficiente.  
Cuando aparece una novedad, el reproceso es la vía rápida y efectiva para corregirla.

---

## 15. Recomendación final

Se recomienda formalizar un canal de comunicación entre:

- DTIC,
- auditoría,
- secretaría,
- y los responsables del flujo documental,

para que cualquier cobertura con novedad sea reprocesada oportunamente y no llegue con errores al destino final.

