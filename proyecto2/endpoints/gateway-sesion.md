
# Gateway y Sesión

## Arquitectura
El gateway es el punto único de entrada de la plataforma, expuesto al host en el puerto 8000.

Los servicios internos se comunican mediante la red backend de Docker Compose. El simulador únicamente puede comunicarse con el
gateway mediante HTTP.

Servicios:
- registro: MongoDB, estado definitivo de tareas.
- sesion: Valkey, locks y borradores temporales.
- historial: almacenamiento de eventos en Parquet.
- analitica: consultas históricas mediante Spark.

## Endpoints públicos

| Método | Ruta | Responsabilidad |
|---|---|---|
| POST | /proyectos | Crear proyecto |
| POST | /proyectos/{id}/lotes | Registrar lote |
| POST | /tareas/siguiente | Asignar tarea y abrir sesión |
| PUT | /tareas/{id}/borrador | Guardar borrador |
| POST | /tareas/{id}/enviar | Confirmar anotación |
| POST | /tareas/{id}/liberar | Liberar tarea |
| POST | /tareas/{id}/revision | Aprobar o rechazar |
| POST | /sesiones/recuperar | Recuperar sesión |
| GET | /analitica/{consulta} | Ejecutar Q1-Q5 |
| GET | /proyectos/{id}/exportar | Exportar aprobadas |
| GET | /salud | Consultar estado general |

Las rutas públicas corresponden al enunciado.
Los servicios internos se limitarán a un máximo
de seis rutas cada uno.

## Integración con registro

El gateway llamará a http://registro:8000.

Las operaciones enviar, liberar y revision se
traducirán a POST /tareas/{id}/transicion con
la accion correspondiente.

Se respetarán los códigos HTTP del registro:
- 200: operación confirmada.
- 201: recurso creado.
- 204: no hay tareas disponibles.
- 404: recurso inexistente.
- 409: conflicto o token incorrecto.
- 422: solicitud inválida.
- 503: servicio no disponible.

Un envío no se reintentará automáticamente si
MongoDB no responde.

## Sesión con Valkey

Valkey utilizará persistencia AOF con everysec.

Estructuras requeridas:
- lock:tarea:{id}: lock exclusivo con SET NX PX.
- sesion:{anotador}: hash de sesión.
- borrador:{tarea}: último borrador.
- editando:{proyecto}: conjunto ordenado.

El lock tendrá una duración inicial de 120 segundos. El borrador se actualizará cada dos segundos y no producirá eventos en el historial.

El servicio permitirá recuperar borradores
después de una interrupción o reinicio de Valkey.
Los locks se liberarán verificando su propietario
para evitar eliminar el lock de otra sesión.

## Envío de anotaciones

El gateway verificará la sesión y el token. Si MongoDB confirma el envío, responderá 200 y continuará el flujo del evento al historial.

Si el estado o token es incorrecto, responderá 409. Si MongoDB no responde, devolverá 503 sin
reintentar automáticamente y conservará el
borrador en Valkey.

## Salud del sistema

GET /salud informará el estado de cada capa:
- operativo
- degradado
- caido

Valkey almacenará mensajes de estado de los
componentes. El gateway deberá detectar
mensajes vencidos y fallos de comunicación.
El gateway seguirá respondiendo a /salud aunque
una dependencia esté caída.

## Pruebas

P1: rutas REST, salud normal, degradada y caída,
aislamiento y exposición de puertos.
P3: recuperación de borradores, reinicio de Valkey con AOF y vencimiento de locks.
P6: 10, 40 y 100 usuarios virtuales, calentamiento
de 30 segundos y tres corridas de 90 segundos por
nivel. Registrar p50, p95, p99, errores, solicitudes por minuto y docker stats.

## Pendientes de integración

- Definir las rutas internas del servicio de sesión.
- Coordinar los latidos con el vencimiento en MongoDB.
- Integrar las interfaces de historial y analítica.
- Acordar el mecanismo de publicación durable
  de eventos después de confirmar escrituras.
- Confirmar el límite de seis rutas cuando el
  gateway público tiene once operaciones.