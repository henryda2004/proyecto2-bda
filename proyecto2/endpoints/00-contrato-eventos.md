# Contrato de eventos del historial

Cada transición de estado de una tarea genera exactamente una fila en el datalake Parquet. El borrador no genera fila, y un segundo envío (HTTP 409) tampoco. El estado de la fila es el que queda en MongoDB, salvo el rechazo, que en MongoDB vuelve a `disponible` y en Parquet queda como `rechazada`.

| Campo | Tipo | Descripción |
| --- | --- | --- |
| `event_id` | string (UUID) | Identificador único del evento. Spark deduplica por este campo |
| `event_ts` | timestamp (UTC) | Momento de la transición. De aquí sale la partición `mes=AAAA-MM` |
| `tarea_id` | string (UUID) | Tarea afectada |
| `version` | int | Versión de la tarea en MongoDB después de la transición |
| `estado` | string | `abierta`, `anotada`, `en_revision`, `aprobada` o `rechazada` |
| `cliente_id` | string (UUID) | Cliente dueño del proyecto (Q4, Q5) |
| `proyecto_id` | string (UUID) | Proyecto de la tarea (Q4) |
| `batch_id` | string (UUID) | Lote al que pertenece la tarea |
| `anotador_id` | string (UUID) | Persona que tomó la tarea |
| `equipo_id` | string | Equipo del anotador, hay 30 (Q2) |
| `revisor_id` | string, nulo | Solo en `en_revision`, `aprobada` y `rechazada` |
| `modalidad` | string | `texto`, `imagen`, `video` o `lidar` (Q1, Q3) |
| `tipo_tarea` | string | `span`, `bbox`, `poligono`, `bbox_video` o `cuboide_3d` (Q3) |
| `duracion_seg` | double, nulo | Tiempo entre `abierta` y `anotada`. Solo en las filas `anotada` (Q3) |
| `motivo_rechazo` | string, nulo | Código del motivo. Solo en `rechazada` |
| `sla_horas` | int | Plazo del proyecto, 48 en el ejemplo (Q5) |

Los campos que no aplican a un estado van en nulo. Los archivos se guardan en `datalake/eventos/mes=AAAA-MM/`, con el mes tomado de `event_ts`.