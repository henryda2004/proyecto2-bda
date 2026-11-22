# Proyecto 2. Plataforma distribuida de anotación de datos

**Curso:** TI-4601 Bases de Datos Avanzadas

**Institución:** Instituto Tecnológico de Costa Rica

**Dominio:** Plataforma de anotación de datos (LiDAR, imagen, video y texto)

**Motores:** MongoDB 7 (registro), Valkey (sesión), Parquet y Apache Spark (historial y analítica)

**Repositorio:** https://github.com/henryda2004/proyecto2-bda

---

## Qué hace cada archivo (registro)

| Archivo | Qué hace |
| --- | --- |
| `docker-compose.yml` | Define los tres nodos de MongoDB (`mongo1`, `mongo2`, `mongo3`) y el servicio `registro`. No publica puertos |
| `docker-compose.dev.yml` | Publica `registro` solo en `127.0.0.1:8001`, para desarrollo y pruebas |
| `scripts/setup.sh` | Levanta los nodos, inicializa el replica set si hace falta, crea la base y los índices, y levanta `registro`. Se puede correr varias veces |
| `servicios/registro/main.py` | Servicio FastAPI con las seis rutas del registro |
| `servicios/registro/db.py` | Conexión a MongoDB con `w: majority` y `wTimeoutMS` de 3000 |
| `servicios/registro/Dockerfile` | Construye la imagen del servicio |
| `servicios/registro/scripts/init-replica.js` | Inicializa el replica set `rs0` |
| `servicios/registro/scripts/init-db.js` | Crea la base `plataforma` y sus colecciones |
| `servicios/registro/scripts/create-indexes.js` | Crea los índices requeridos |
| `servicios/registro/scripts/bench-write-concern.js` | Compara la latencia de escritura con `w: 1` y `w: majority` |
| `endpoints/registro.md` | Contrato de las rutas internas del registro, con ejemplos reales y decisiones de diseño |
| `endpoints/00-contrato-eventos.md` | Campos de cada evento que va al datalake |
| `dataset/catalogo.py` | Catálogo determinista de clientes, proyectos y anotadores, compartido con el historial |
| `dataset/generar_registro.py` | Genera los archivos JSONL del registro en `datos/registro/` |
| `dataset/cargar_registro.sh` | Carga los JSONL en MongoDB y escribe los conteos |
| `pruebas/p1_salud_registro.py` | Prueba `/salud` del registro en estado operativo, degradado y caído |
| `pruebas/p1_aislamiento_registro.py` | Comprueba que el registro no menciona Valkey, qué servicios tienen configuración de MongoDB y qué puertos se publican |
| `pruebas/p2_concurrencia.py` | 50 solicitudes simultáneas contra 10 tareas, más las pruebas de token y de segundo envío |
| `pruebas/p2_indices.py` | Compara cada consulta con y sin índice usando `explain` |
| `pruebas/p8_fallas_mongo.py` | Detiene un secundario, el primario y dos nodos, y mide el RTO y el RPO |
| `pruebas/ejemplos_registro.py` | Recorre las seis rutas y guarda cada respuesta real |
| `evidence/p1-salud-registro.json` | Resultado de la prueba de salud en los tres estados |
| `evidence/p1-aislamiento-registro.json` | Resultado de la revisión de aislamiento y puertos |
| `evidence/p2-write-concern.json` | Latencias con `w: 1` y con `w: majority` |
| `evidence/p2-concurrencia.json` | Resultado de la prueba de 50 contra 10 |
| `evidence/p2-indices-explain.json` | Documentos examinados con y sin índice |
| `evidence/p8-fallas-mongo.json` | Resultado de los tres escenarios de falla |
| `evidence/registro-ejemplos.json` | Peticiones y respuestas reales de cada ruta |
| `evidence/dataset-registro-conteos.txt` | Conteos del dataset cargado |
| `evidence/machine-specs.txt` | Características de la máquina donde se midió |

---

## Cómo levantar el registro

En Windows (PowerShell), usando Git Bash:
```powershell
& "C:\Program Files\Git\bin\bash.exe" proyecto2/scripts/setup.sh
```

Al terminar debe aparecer `registro responde en /salud` y el replica set con un nodo `PRIMARY` y dos `SECONDARY`.

Para cargar el dataset del registro:
```powershell
python proyecto2/dataset/generar_registro.py
& "C:\Program Files\Git\bin\bash.exe" proyecto2/dataset/cargar_registro.sh
```

Para correr las pruebas del registro, desde la raíz del repositorio:
```powershell
python proyecto2/pruebas/p1_salud_registro.py
python proyecto2/pruebas/p1_aislamiento_registro.py
python proyecto2/pruebas/p2_concurrencia.py
python proyecto2/pruebas/p2_indices.py
python proyecto2/pruebas/p8_fallas_mongo.py
```

`p2_indices.py` necesita el dataset cargado. Las pruebas de salud y de fallas detienen nodos de MongoDB y los restauran al terminar. `p8_fallas_mongo.py` tarda unos 5 minutos. Si alguna se interrumpe a mitad, hay que correr `setup.sh` otra vez.


---
## Gateway, sesión y simulador

Los comandos de esta sección se ejecutan usando WSL con Docker Compose, Python 3 y `curl`.

### Arranque

En un entorno nuevo, inicializar el registro y cargar el dataset.

```bash
bash scripts/setup.sh
python3 dataset/generar_registro.py
bash dataset/cargar_registro.sh
```

El script de carga vacía y repuebla las colecciones del registro, con el dataset ya cargado, se pruede proceder al arranque de los servicios.

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml \
  -f docker-compose.gateway.yml up -d --build valkey sesion gateway
```

Cuando arranque el gateway, consultar `/salud` y verificar que las capas `registro` y `sesion` figuren operativas.

```bash
curl -fsS http://127.0.0.1:8000/salud
```

El servicio de sesión usa Valkey en la red `backend`. El volumen `valkey_data` conserva los datos de Valkey con AOF y `appendfsync everysec`. El [contrato de sesión](endpoints/sesion.md) describe las rutas de asignación, guardado, recuperación y liberación.

### Preparar una tarea de prueba para P3 y P8

Con los servicios operativos y el dataset cargado, crear por el gateway un proyecto de texto con una tarea disponible. El bloque usa un cliente y dos anotadores registrados del catálogo.

```bash
python3 - <<'PY'
from dataset.catalogo import generar_catalogo
from pruebas.sesion_basica import llamar

catalogo = generar_catalogo()
anotador_a, anotador_b = [
    a["_id"] for a in catalogo["anotadores"] if not a["es_revisor"]
][:2]

proyecto = llamar("POST", "/proyectos", {
    "cliente_id": catalogo["clientes"][0]["_id"],
    "nombre": "Pruebas de sesión P3 y P8",
    "modalidad": "texto",
    "taxonomia": [{"nombre": "persona", "instruccion": "Marcar los nombres de personas"}],
}, 201)
proyecto_id = proyecto["id"]

llamar("POST", f"/proyectos/{proyecto_id}/lotes", {
    "nombre": "Lote de prueba de sesión",
    "tareas": [{
        "uri": "s3://pruebas-gateway/texto-01.txt",
        "sha256": "2206ed2a8d55975a13ef63e473db1c8b7649d251992390561a521bb9d41c125e",
        "bytes": 21,
        "mime": "text/plain",
        "meta": {"idioma": "es"},
        "tipo_tarea": "span",
    }],
}, 201)

print(f"PROYECTO_ID={proyecto_id}")
print(f"ANOTADOR_A={anotador_a}")
print(f"ANOTADOR_B={anotador_b}")
PY
```

Si el gateway devuelve `404` con `El anotador no existe`, comprobar que los anotadores del catálogo estén cargados en MongoDB.

### P8 de Valkey y P3 de sesión

Ejecutar las pruebas en orden. P8 interrumpe el cliente, reinicia Valkey y verifica el mismo borrador. P3 espera el vencimiento del candado, comprueba la reasignación de la misma tarea y verifica los códigos `409` con el token anterior y `200` con el nuevo.


```bash
(
set -euo pipefail
mkdir -p evidence

python3 -u pruebas/p8_fallas_valkey.py "$PROYECTO_ID" "$ANOTADOR_A" \
  2>&1 | tee "evidence/p8-fallas-valkey-$(date -u +%Y%m%dT%H%M%SZ).txt"

python3 -u pruebas/p3_vencimiento_candado.py "$PROYECTO_ID" "$ANOTADOR_A" "$ANOTADOR_B" \
  2>&1 | tee "evidence/p3-vencimiento-candado-$(date -u +%Y%m%dT%H%M%SZ).txt"
)
```

Los mensajes finales esperados son `P8 Valkey OK` y `P3 OK`. Ambas pruebas liberan la tarea al finalizar.

### P6 de carga

Correr el simulador con el dataset cargado. Repetir el bloque para `10`, `40` y `100` usuarios, cambiando `usuarios` y dejando terminar cada nivel antes de iniciar el siguiente. Cada nivel ejecuta tres corridas con 30 segundos de calentamiento y 90 segundos de medición por corrida.

```bash
(
set -euo pipefail
usuarios=10
mkdir -p evidence
compose=(docker compose -f docker-compose.yml -f docker-compose.dev.yml
         -f docker-compose.gateway.yml)
ids=$("${compose[@]}" ps -q)
if [[ -z "$ids" ]]; then
  echo "Inicie los servicios de proyecto2 antes de medir la carga." >&2
  exit 1
fi
mapfile -t contenedores <<< "$ids"

salida="evidence/p6-$usuarios-$(date -u +%Y%m%dT%H%M%SZ)"
stats="$salida-docker-stats.txt"
docker info --format 'Docker={{.ServerVersion}} CPU_logicas={{.NCPU}} RAM_bytes={{.MemTotal}}' \
  > "$stats"

python3 -m simulador.carga --usuarios "$usuarios" \
  --calentamiento 30 --duracion 90 --corridas 3 > "$salida.json" &
pid=$!
trap 'kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

while kill -0 "$pid" 2>/dev/null; do
  {
    date -u '+%Y-%m-%dT%H:%M:%SZ'
    docker stats --no-stream "${contenedores[@]}"
  } >> "$stats"
  sleep 5
done

wait "$pid"
trap - EXIT INT TERM
python3 -m json.tool "$salida.json" >/dev/null
printf 'Archivos guardados\n%s\n%s\n' "$salida.json" "$stats"
)
```

El JSON contiene los envíos confirmados, la tasa por minuto, los errores y los percentiles `p50`, `p95` y `p99` por endpoint. El TXT conserva las muestras de CPU y memoria por contenedor. Las corridas consumen tareas del dataset, por lo que una recarga entre niveles se debe registrar en el informe.

### Evidencias de gateway y sesión

| Prueba | Evidencia |
| --- | --- |
| Salud del gateway | [Respuesta de /salud](evidence/gateway-salud-con-sesion.txt) |
| P3 · Vencimiento del candado | [Reasignación y validación de tokens](evidence/p3-vencimiento-candado-20261108T023448Z.txt) |
| P6 · 10 usuarios | [JSON](evidence/p6-10-20261107T235400Z.json) · [Docker stats](evidence/p6-10-20261107T235400Z-docker-stats.txt) |
| P6 · 40 usuarios | [JSON](evidence/p6-40-20261108T001656Z.json) · [Docker stats](evidence/p6-40-20261108T001656Z-docker-stats.txt) |
| P6 · 100 usuarios | [JSON](evidence/p6-100-20261108T003819Z.json) · [Docker stats](evidence/p6-100-20261108T003819Z-docker-stats.txt) |
| P8 · Reinicio de Valkey | [Recuperación del borrador](evidence/p8-fallas-valkey-20261108T022433Z.txt) |



---

## Para apagar todo

Para detener los contenedores sin borrar los datos:
```powershell
docker compose -f proyecto2/docker-compose.yml -f proyecto2/docker-compose.dev.yml down
```

Para borrar también los volúmenes y empezar desde cero:
```powershell
docker compose -f proyecto2/docker-compose.yml -f proyecto2/docker-compose.dev.yml down -v
```

---

## Estado actual (registro)

- MongoDB 7 en replica set de tres nodos, con escritura `w: majority`.
- Seis rutas internas, documentadas en `endpoints/registro.md`.
- `/salud` distingue operativo, degradado y caído según el estado del replica set.
- Asignación atómica de tareas y transiciones condicionales, con 409 ante un segundo envío o un token distinto.
- Cuatro índices requeridos, justificados con `explain` en `evidence/p2-indices-explain.json`.
- Dataset de 60 clientes, 120 proyectos, 600 lotes, 60,000 tareas y 300 anotadores.
- Prueba de 50 solicitudes contra 10 tareas sin dobles asignaciones.
- Pruebas de falla de MongoDB con tres escenarios, sin tareas confirmadas perdidas.
- El registro no mantiene configuración de Valkey ni publica puertos en el compose base.

## Parte 3 — Historial y Datalake

### Archivos implementados

| Archivo | Qué hace |
|---|---|
| `docker-compose.historial.yml` | Define el servicio Historial, publica el puerto `8003`, configura el volumen persistente de SQLite y monta el Datalake local. |
| `servicios/historial/main.py` | Servicio FastAPI encargado de recibir, validar, normalizar y procesar eventos históricos, tanto individualmente como en lotes. |
| `servicios/historial/db.py` | Gestiona SQLite, almacenamiento de eventos, deduplicación mediante `event_id`, eventos pendientes y procesados, y operaciones batch de inserción. |
| `servicios/historial/parquet.py` | Procesa los eventos pendientes mediante flush, los agrupa por mes y genera archivos Parquet comprimidos con Snappy. |
| `servicios/historial/requirements.txt` | Dependencias del servicio: FastAPI, Uvicorn, Pydantic y PyArrow. |
| `servicios/historial/Dockerfile` | Construye la imagen del servicio Historial utilizando Python 3.12. |
| `servicios/historial/generador/generar_historial.py` | Generador de datos sintéticos que simula ciclos de vida de tareas, produce eventos históricos y los envía al servicio mediante lotes HTTP. Permite configurar el volumen de tareas, tamaño de lote, seed y ejecución en modo `dry-run`. |
| `datalake/eventos/` | Directorio local donde se almacenan los eventos históricos en archivos Parquet, particionados por mes. |
| `evento_prueba.json` | Evento sintético utilizado para probar la recepción, validación y persistencia de un evento individual. |
| `evento_prueba2.json` | Segundo evento sintético utilizado para comprobar el flush manual. |

### Arquitectura de Historial

El servicio Historial recibe los eventos mediante HTTP utilizando FastAPI. Los eventos son validados y normalizados antes de almacenarse inicialmente en SQLite, donde se mantienen como pendientes de procesamiento.

La inserción utiliza operaciones batch para reducir el overhead de procesamiento y las transacciones de SQLite. Se implementa deduplicación mediante `event_id`, evitando almacenar eventos que ya hayan sido recibidos.

Posteriormente, los eventos pendientes se agrupan por mes y se escriben en archivos Parquet dentro del Datalake. Una vez completada la escritura, los eventos correspondientes se marcan como procesados en SQLite.

El procesamiento utiliza dos tamaños de batch independientes:

| Variable | Valor | Descripción |
|---|---:|---|
| `BATCH_SIZE` | `1000` | Cantidad de eventos enviada en cada lote HTTP al servicio Historial. |
| `FLUSH_BATCH_SIZE` | `10000` | Umbral de eventos pendientes que activa el flush automático hacia Parquet. |

Esta separación permite recibir eventos mediante lotes HTTP relativamente pequeños, mientras se acumulan eventos en SQLite para generar archivos Parquet de mayor tamaño y reducir la cantidad de archivos producidos.

El flujo de procesamiento es:

1. Recibir eventos mediante FastAPI.
2. Validar y normalizar los eventos según el contrato establecido.
3. Insertar el lote en SQLite, evitando duplicados mediante `event_id`.
4. Mantener los eventos nuevos como pendientes de procesamiento.
5. Cuando el número de eventos pendientes alcanza el umbral de `FLUSH_BATCH_SIZE`, ejecutar un flush automático.
6. Agrupar los eventos pendientes por mes.
7. Generar los archivos Parquet correspondientes.
8. Marcar como procesados en SQLite los eventos cuya escritura al Datalake se completó correctamente.

### Particionamiento del Datalake

Los archivos Parquet se almacenan utilizando particionamiento mensual mediante el campo `mes`, siguiendo la estructura:

```text
datalake/
└── eventos/
    ├── mes=2026-11/
    │   ├── batch_58c7b08ad1bb0cf0.parquet
    │   └── batch_XXXXXXXXXXXXXXXX.parquet
    ├── mes=2026-12/
    │   └── batch_XXXXXXXXXXXXXXXX.parquet
    └── ...
```

Cada archivo contiene los 16 campos definidos en el contrato de eventos. El campo `mes` se utiliza como partición del directorio y no forma parte del esquema físico de las columnas Parquet.

Los archivos se escriben utilizando compresión Snappy. Para evitar exponer archivos parcialmente escritos, se utiliza un archivo temporal con extensión `.tmp`, que posteriormente se renombra al archivo definitivo mediante `os.replace()`.

El agrupamiento por mes permite organizar el historial temporalmente y facilita el procesamiento posterior mediante herramientas analíticas como Apache Spark.

### Cómo levantar el servicio Historial

Desde la raíz del repositorio `proyecto2`, ejecutar:

```bash
docker compose -f docker-compose.historial.yml up -d --build
```

Para verificar que el contenedor está funcionando:

```bash
docker ps
```

El contenedor debe aparecer como `historial`.

Consultar el endpoint de salud:

```bash
curl http://localhost:8003/salud
```

Respuesta esperada:

```json
{
  "servicio": "historial",
  "estado": "ok"
}
```

### Endpoints implementados

| Método | Endpoint | Descripción |
|---|---|---|
| `GET` | `/salud` | Verifica que el servicio esté disponible. |
| `POST` | `/eventos` | Recibe, valida y almacena un evento individual. |
| `POST` | `/eventos/lote` | Recibe una lista de eventos, los normaliza e inserta mediante procesamiento batch. |
| `POST` | `/flush` | Fuerza el procesamiento de eventos pendientes y su escritura a Parquet. |
| `GET` | `/estadisticas` | Devuelve la cantidad de eventos totales, pendientes y procesados. |

### Prueba de recepción de eventos

Enviar el evento sintético:

```bash
curl -X POST http://localhost:8003/eventos \
  -H "Content-Type: application/json" \
  -d @evento_prueba.json
```

La respuesta indica si el evento fue recibido y si se trata de un duplicado.

Ejemplo de respuesta:

```json
{
  "event_id": "9c2a4f10-8b71-4e53-a1d9-6f52a8c30101",
  "recibido": true,
  "duplicado": false
}
```

Para comprobar la deduplicación, enviar nuevamente el mismo evento:

```bash
curl -X POST http://localhost:8003/eventos \
  -H "Content-Type: application/json" \
  -d @evento_prueba.json
```

El evento debe ser reconocido como duplicado y no debe incrementar el número de registros almacenados.

### Consultar estadísticas

```bash
curl http://localhost:8003/estadisticas
```

La respuesta permite verificar:

- Total de eventos almacenados.
- Cantidad de eventos pendientes de procesamiento.
- Cantidad de eventos procesados.

Estas estadísticas permiten comprobar el estado de la ingestión y del procesamiento de los eventos en SQLite.

### Flush manual

Para procesar los eventos pendientes y forzar su escritura a Parquet:

```bash
curl -X POST http://localhost:8003/flush
```

El servicio devuelve el número de eventos procesados y las rutas de los archivos Parquet generados.

Después del flush, volver a consultar las estadísticas:

```bash
curl http://localhost:8003/estadisticas
```

Los eventos escritos correctamente deben aparecer como procesados y dejar de contabilizarse como pendientes.

### Flush automático

El servicio ejecuta un flush al iniciar, permitiendo intentar recuperar eventos que hayan quedado pendientes en SQLite de una ejecución anterior. Esto permite continuar el procesamiento después de un reinicio del contenedor, siempre que existan eventos pendientes y los archivos correspondientes no hayan sido generados previamente.

Además, cuando la cantidad de eventos pendientes alcanza el valor configurado en `FLUSH_BATCH_SIZE` (actualmente `10000`), el servicio intenta realizar un flush automático.

El flush extrae hasta el límite configurado de eventos pendientes, los agrupa por mes, genera los archivos Parquet correspondientes y marca los eventos como procesados una vez completada la escritura.

El flush final solicitado por el generador puede devolver cero eventos si los flush automáticos ya procesaron todos los eventos pendientes durante la ingestión.

### Generador de historial sintético

Se implementó un generador de eventos sintéticos para simular el ciclo de vida de tareas de anotación y producir un historial de actividad a gran escala.

Archivo:

```text
servicios/historial/generador/generar_historial.py
```

El generador utiliza el catálogo sintético del proyecto para reutilizar entidades como clientes, proyectos, equipos, anotadores y revisores. Los IDs de las tareas y batches generados para el historial son propios del generador y no modifican el registro operativo de MongoDB.

Cada tarea genera una secuencia de eventos que representa su ciclo de vida, incluyendo apertura, anotación, revisión, rechazos cuando corresponda, reasignación y aprobación final.

La distribución de ciclos utilizada en las pruebas es:

| Ciclo | Porcentaje | Tareas |
|---|---:|---:|
| 0 rechazos | 25% | 25,000 por cada 100,000 tareas |
| 1 rechazo | 50% | 50,000 por cada 100,000 tareas |
| 2 rechazos | 25% | 25,000 por cada 100,000 tareas |

El número de eventos por tarea depende de su ciclo:

| Rechazos | Eventos por tarea |
|---|---:|
| 0 | 4 |
| 1 | 8 |
| 2 | 12 |

Con esta distribución se obtiene un promedio de 8 eventos por tarea, permitiendo generar 800,000 eventos a partir de 100,000 tareas.

El generador permite configurar los parámetros de ejecución, por ejemplo:

```bash
python servicios/historial/generador/generar_historial.py \
  --tareas 100000 \
  --batch-size 1000 \
  --seed 5126
```

También permite ejecutar una simulación sin enviar eventos al servicio utilizando `--dry-run`, así como ajustar la cantidad de tareas, tamaño de batch, URL del servicio y seed.

### Prueba de carga del generador sintético

Se realizó una prueba de carga utilizando 100,000 tareas sintéticas, generando un total de 800,000 eventos históricos.

Resultados de la ejecución:

| Métrica | Resultado |
|---|---:|
| Tareas generadas | 100,000 |
| Eventos generados | 800,000 |
| Eventos enviados | 800,000 |
| Lotes HTTP | 800 |
| Tamaño de batch HTTP | 1,000 |
| Tiempo total | 364.47 segundos |
| Tiempo aproximado | 6 min 4.47 s |
| Throughput promedio | 2,195 eventos/s |

Distribución de ciclos por cantidad de rechazos:

| Rechazos | Tareas |
|---|---:|
| 0 | 25,000 |
| 1 | 50,000 |
| 2 | 25,000 |

Distribución de eventos por estado:

| Estado | Eventos |
|---|---:|
| `abierta` | 200,000 |
| `anotada` | 200,000 |
| `en_revision` | 200,000 |
| `aprobada` | 100,000 |
| `rechazada` | 100,000 |
| **Total** | **800,000** |

Distribución de eventos por modalidad:

| Modalidad | Eventos |
|---|---:|
| `imagen` | 240,372 |
| `lidar` | 161,200 |
| `texto` | 238,108 |
| `video` | 160,320 |
| **Total** | **800,000** |

Los eventos se distribuyeron temporalmente a lo largo del período sintético de 12 meses, utilizando particionamiento mensual en el Datalake.

El throughput promedio de la prueba fue de aproximadamente 2,195 eventos por segundo. El tiempo de ejecución se mantuvo proporcional al aumentar el volumen de la prueba desde 80,000 hasta 800,000 eventos, sin observar una degradación significativa del throughput bajo las condiciones de ejecución utilizadas.

El flush final devolvió cero eventos pendientes, debido a que los flush automáticos habían procesado los eventos acumulados durante la ingestión.

### Verificar los archivos Parquet generados

Desde la raíz del repositorio:

```bash
find datalake/eventos -type f
```

Para consultar el esquema físico de uno de los archivos Parquet:

```bash
docker exec historial python -c "
import glob
import pyarrow.parquet as pq

f = glob.glob('/datalake/eventos/mes=*/batch_*.parquet')[0]
p = pq.ParquetFile(f)

print('Archivo:', f)
print('Esquema:')
print(p.schema)
print('Cantidad de filas:', p.metadata.num_rows)
"
```

Para leer el contenido de un archivo Parquet:

```bash
docker exec historial python -c "
import glob
import pyarrow.parquet as pq

f = glob.glob('/datalake/eventos/mes=*/batch_*.parquet')[0]
table = pq.read_table(f)

print(table.schema)
print(table.to_pandas())
"
```

Para verificar el particionamiento mensual:

```bash
find datalake/eventos -maxdepth 1 -type d
```

### Persistencia

El servicio utiliza dos mecanismos de persistencia:

- **SQLite:** almacena los eventos recibidos, su payload y su estado de procesamiento en el volumen Docker `historial_data`.
- **Datalake:** almacena los archivos Parquet en `datalake/eventos/`, montado directamente desde el repositorio.

El volumen de SQLite permite conservar los registros de eventos pendientes y procesados aunque el contenedor sea recreado. El Datalake permanece en el directorio local del repositorio, separado del ciclo de vida del contenedor.

SQLite mantiene el control de los eventos recibidos y su estado, mientras que Parquet constituye el almacenamiento histórico destinado al procesamiento analítico posterior.

### Estado actual — Historial

- Servicio FastAPI implementado y operativo.
- Contenedor Docker construido y ejecutándose correctamente.
- Endpoint de salud verificado.
- Recepción de eventos individuales validada.
- Recepción batch implementada y optimizada.
- Deduplicación por `event_id` comprobada.
- Inserción batch en SQLite implementada.
- Persistencia inicial en SQLite verificada.
- Escritura de archivos Parquet comprobada.
- Esquema físico Parquet verificado con los 16 campos del contrato.
- Particionamiento mensual `mes=YYYY-MM` comprobado.
- Compresión Snappy implementada.
- Escritura mediante archivos temporales y renombrado atómico implementada.
- Flush automático al iniciar implementado.
- Flush automático mediante umbral de `FLUSH_BATCH_SIZE` implementado.
- Flush manual probado exitosamente.
- Configuración independiente de batch HTTP y tamaño de flush implementada.
- Generador de historial sintético implementado.
- Prueba de carga de 100,000 tareas completada.
- 800,000 eventos generados y enviados exitosamente.
- Rendimiento aproximado de 2,195 eventos por segundo en la prueba de carga.
- Integración con el Gateway pendiente.
- Validación integral de consistencia entre SQLite y el total del Datalake pendiente de confirmar.
- Consultas analíticas con Spark pendientes.
