# Registro, rutas internas

Servicio `registro` (FastAPI). Solo es accesible dentro de la red `backend`, el gateway lo llama en `http://registro:8000`. Para desarrollo se publica en `127.0.0.1:8001` con `docker-compose.dev.yml`.

Son seis rutas, el máximo permitido por servicio. Todos los cuerpos son JSON. Los errores devuelven `{"detail": ...}`. Si MongoDB no logra confirmar una escritura con mayoría en 3 segundos, cualquier ruta responde `503`. Los ejemplos de abajo son respuestas reales guardadas en `evidence/registro-ejemplos.json`, y el comportamiento ante fallas está en `evidence/p8-fallas-mongo.json`.

| Método | Ruta | Para qué |
|---|---|---|
| GET | /salud | Estado del servicio |
| POST | /proyectos | Crear un proyecto |
| POST | /proyectos/{proyecto_id}/lotes | Cargar un lote de tareas |
| POST | /tareas/siguiente | Asignar la siguiente tarea a un anotador |
| POST | /tareas/{tarea_id}/transicion | Cambiar el estado de una tarea |
| GET | /proyectos/{proyecto_id}/exportar | Exportar tareas aprobadas, paginado |

## GET /salud

Consulta el estado del replica set de MongoDB y responde con `servicio`, `estado`, `nodos_sanos` y `nodos_total`.

| Estado | HTTP | Cuándo |
| --- | --- | --- |
| operativo | 200 | Los tres nodos están sanos |
| degradado | 200 | Hay primario y dos nodos sanos, todavía se puede escribir con mayoría |
| caido | 503 | No hay primario, queda menos de dos nodos sanos o no hay conexión. En ese caso `nodos_sanos` es 0 porque no se puede leer el estado |

Ejemplo en estado normal.

```json
{"servicio": "registro", "estado": "operativo", "nodos_sanos": 3, "nodos_total": 3}
```

Las respuestas de los tres estados están en `evidence/p1-salud-registro.json`.

## POST /proyectos

Cuerpo. `cliente_id`, `nombre`, `modalidad` (`texto`, `imagen`, `video` o `lidar`), `taxonomia` (lista con al menos una etiqueta `{nombre, instruccion}`), `reglas` (opcional) y `sla_horas` (opcional, 48 por defecto).

Respuestas. `201` con `{"id": "...", "estado": "activo"}`. `404` si el cliente no existe (`"El cliente no existe"`). `422` si el cuerpo es inválido.

## POST /proyectos/{proyecto_id}/lotes

Cuerpo. `nombre` y `tareas`, de 1 a 5000 elementos. Cada tarea lleva `uri`, `sha256` (64 caracteres hexadecimales en minúscula), `bytes` (mayor que 0), `mime`, `meta` (opcional), `tipo_tarea` (`span`, `bbox`, `poligono`, `bbox_video` o `cuboide_3d`, coherente con la modalidad del proyecto) y `prioridad` (opcional, 0 por defecto).

La inserción es una transacción, entra todo el lote o no entra nada.

Respuestas. `201` con `{"id": "<id del lote>", "tareas": 3}`. `404` si el proyecto no existe. `422` si el cuerpo es inválido.

## POST /tareas/siguiente

Cuerpo. `anotador_id`, `proyecto_id` y `plazo_seg` (opcional, 120 por defecto).

La asignación es atómica. Entrega la tarea disponible de mayor prioridad, o una tarea abierta cuyo plazo (`vence_en`) ya venció. Dos anotadores nunca reciben la misma tarea.

Respuestas. `200` con `tarea` (id, proyecto_id, uri, sha256, bytes, mime, meta, modalidad, tipo_tarea, taxonomia, reglas), `token`, `vence_en` y `evento`. `204` sin cuerpo si no hay tareas disponibles. `404` si el proyecto no existe.

El `token` hay que enviarlo de vuelta en `enviar` y `liberar`.

## POST /tareas/{tarea_id}/transicion

Cuerpo. `accion` más los campos de cada acción.

| accion | Campos | Estado resultante |
|---|---|---|
| enviar | anotador_id, token, anotacion | anotada |
| iniciar_revision | revisor_id | en_revision |
| aprobar | revisor_id | aprobada |
| rechazar | revisor_id, motivo | disponible |
| liberar | anotador_id, token | disponible |

Respuesta `200` con `{"tarea_id", "estado", "version", "evento"}`. `409` si la tarea no está en un estado que permita la acción o el token no corresponde. Esto cubre un segundo envío de la misma tarea.

Dos detalles. Al rechazar, la tarea vuelve a `disponible` en el registro, pero el evento que se genera tiene `estado: "rechazada"` y el `motivo_rechazo`. Al liberar no se genera evento, y `evento` viene en `null`.

## GET /proyectos/{proyecto_id}/exportar

Parámetros de consulta. `limite` (tamaño de página) y `despues` (cursor).

Devuelve solo tareas aprobadas.

```json
{
  "tareas": [
    {
      "id": "...", "uri": "...", "sha256": "...", "bytes": 1048576,
      "mime": "...", "meta": {}, "modalidad": "lidar", "tipo_tarea": "cuboide_3d",
      "anotacion": {}, "revisado_por": "rev-ejemplo", "aprobada_en": "2026-11-02T21:16:32.370000+00:00"
    }
  ],
  "limite": 2,
  "siguiente": null
}
```

Si `siguiente` no es `null`, se pasa como `despues` para pedir la página siguiente. Responde `404` si el proyecto no existe.

## Eventos

Cada respuesta con `evento` trae la fila lista para el histórico, con los campos definidos en `00-contrato-eventos.md`.


## Decisiones de diseño

La liberación de una tarea no genera evento. Los estados que admite una fila de Parquet son abierta, anotada, en_revision, aprobada y rechazada, y una tarea liberada queda en disponible, que no es uno de ellos. Tampoco aparece entre las transiciones que el enunciado lista como generadoras de fila.

El plazo `vence_en` no se renueva. El registro no consulta Valkey y las rutas están limitadas a seis, así que el gateway debe enviar `plazo_seg` igual al tiempo de vida del lock de sesión. Si una tarea se reasigna mientras el anotador anterior sigue editando, el envío de ese anotador recibe 409, porque cada asignación genera un token nuevo.