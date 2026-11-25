# Proyecto 2. Plataforma distribuida de anotación de datos

**Curso:** TI-4601 Bases de Datos Avanzadas

**Institución:** Instituto Tecnológico de Costa Rica

**Dominio:** Plataforma de anotación de datos (LiDAR, imagen, video y texto)

**Motores:** MongoDB 7 (registro), Valkey (sesión), Parquet y Apache Spark (historial y analítica)

**Repositorio:** https://github.com/henryda2004/proyecto2-bda

---

## Inicio rápido (entorno limpio)

Requisitos previos: Docker Desktop con Docker Compose v2, Python 3.11 o superior, Git y, en Windows, Git Bash para ejecutar los `.sh`. Todos los comandos se ejecutan desde la carpeta `proyecto2/`.

1. Levantar todo el sistema (MongoDB en réplica, registro, sesión con Valkey, historial, analítica y gateway):
   `bash scripts/setup.sh`
2. Cargar los datos del registro y del historial: ver `datos/README.md`.
3. Verificar la salud por capa: `curl http://127.0.0.1:8000/salud` debe responder `operativo` en registro, sesión, historial y analítica.
4. Recorrido completo de una tarea LiDAR con evidencia (P4): `python pruebas/p4_recorrido_lidar.py`.
5. Consultas analíticas por el gateway: `curl http://127.0.0.1:8000/analitica/q1` (q1 a q5).

Solo el gateway publica un puerto al host (8000). Registro (8001), historial (8003) y analítica (8004) se publican únicamente en 127.0.0.1 para administración; MongoDB y Valkey no se publican.

Para apagar: `docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.gateway.yml -f docker-compose.historial.yml -f docker-compose.analitica.yml down` (agregar `-v` para borrar los volúmenes).

### Solución de problemas

- `analitica` aparece `caido` y su log dice `UNABLE_TO_INFER_SCHEMA`: el datalake está vacío. Genere el historial (`datos/README.md`) y reinicie `analitica`.
- Docker Desktop en Windows indica que Virtual Machine Platform no está habilitado o que WSL no responde: habilite la característica, ejecute `wsl --update` y `wsl --shutdown`, y reinicie Docker Desktop.
- `setup.sh` falla con rutas duplicadas: ejecútelo desde `proyecto2/` o con la ruta `scripts/setup.sh`; el script cambia solo a la carpeta correcta.
- Un `curl` en PowerShell devuelve HTML o pide parámetros: use `curl.exe` en lugar de `curl`.
- Analítica tarda en responder después de arrancar: Spark lee y deduplica el datalake al iniciar; espere a que `/salud` la marque `operativo`.

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
- Servicio de Analítica con Apache Spark implementado y operativo.
- Integración de Spark con el Datalake Parquet de Historial completada.
- Dataset histórico leído desde el Datalake y deduplicado mediante `event_id`.
- Dataset de eventos materializado y persistido en caché de Spark (`MEMORY_AND_DISK`).
- Inicialización del dataset durante el arranque del servicio de Analítica.
- Endpoint de estadísticas de Analítica implementado y verificado.
- Consultas analíticas Q1–Q5 implementadas y ejecutadas exitosamente.
- Optimización de consultas mediante reutilización del dataset persistido.
- Rendimiento de las consultas Q1–Q5 medido y verificado.

## Parte 4 — Analítica con Apache Spark

### Objetivo

Se implementó un servicio de analítica utilizando Apache Spark para procesar los eventos históricos almacenados en el Datalake de Parquet generado por el servicio Historial.

El servicio permite ejecutar consultas analíticas sobre el historial de las tareas, aprovechando el procesamiento distribuido de datos de Spark y su capacidad para realizar operaciones de filtrado, agrupamiento, agregación, joins y cálculo de métricas estadísticas.

El objetivo es separar la ingestión y persistencia de eventos del procesamiento analítico, permitiendo que el servicio Historial se encargue de almacenar los eventos, mientras que Analítica se encarga de procesarlos para obtener indicadores y estadísticas.

---

### Archivos implementados

| Archivo | Qué hace |
|---|---|
| `docker-compose.analitica.yml` | Define el servicio Analítica, publica el puerto `8004` y monta el Datalake de eventos como volumen de solo lectura. |
| `servicios/analitica/main.py` | Servicio FastAPI que expone los endpoints de estadísticas y consultas analíticas Q1–Q5. |
| `servicios/analitica/consultas.py` | Contiene la configuración de Spark, lectura del Datalake, preparación del dataset y las implementaciones de Q1–Q5. |
| `servicios/analitica/requirements.txt` | Dependencias del servicio: FastAPI, Uvicorn y PySpark. |
| `servicios/analitica/Dockerfile` | Construye la imagen del servicio Analítica, incluyendo Python y Java 17, necesarios para ejecutar Spark. |
| `pruebas/analitica_controlada.py` | Script utilizado para validar la implementación de Q1–Q5 y realizar pruebas de rendimiento controladas. |
| `evidence/validacion_analitica.txt` | Evidencia de validación funcional de las cinco consultas analíticas. |
| `evidence/benchmark_analitica.txt` | Resultados de las pruebas de rendimiento y comparación de configuración de workers. |
| `evidence/resumen_analitica.txt` | Resumen de los principales resultados experimentales obtenidos. |

---

### Arquitectura del servicio Analítica

El servicio Analítica utiliza FastAPI como interfaz HTTP y Apache Spark como motor de procesamiento de datos.

El Datalake generado por Historial se monta dentro del contenedor de Analítica en la ruta:

```text
/datalake/eventos
```

El servicio utiliza PySpark para leer los archivos Parquet y preparar un DataFrame con los eventos históricos que serán utilizados por las consultas analíticas.

La arquitectura implementada permite mantener separados los servicios de ingestión y analítica:

- **Historial:** recibe, valida y persiste los eventos.
- **Datalake:** almacena los eventos históricos en archivos Parquet particionados por mes.
- **Analítica:** lee el Datalake y ejecuta las consultas sobre el historial almacenado.

El volumen del Datalake se monta en Analítica en modo de solo lectura (`ro`), evitando que las consultas analíticas modifiquen directamente los archivos históricos.

---

### Configuración de Apache Spark

Spark se configura para ejecutarse en modo local dentro del contenedor de Analítica.

La configuración utilizada incluye:

| Parámetro | Valor | Descripción |
|---|---|---|
| `master` | `local[1]` | Ejecuta Spark utilizando un worker local. |
| `SPARK_WORKERS` | `1` | Define la cantidad de threads de ejecución local utilizados por Spark. |
| `spark.sql.shuffle.partitions` | `1` | Configura la cantidad de particiones para operaciones de shuffle. |
| `spark.sql.adaptive.enabled` | `true` | Habilita Adaptive Query Execution. |
| `spark.sql.parquet.filterPushdown` | `true` | Permite optimizar el filtrado de datos durante la lectura de Parquet. |

La configuración permite ejecutar las consultas analíticas dentro del contenedor sin necesidad de desplegar un clúster Spark externo.

---

### Lectura y preparación del dataset

Durante la inicialización del servicio, Spark lee los archivos Parquet almacenados en el Datalake:

```text
/datalake/eventos
```

El DataFrame resultante se prepara mediante los siguientes pasos:

1. Leer los archivos Parquet del Datalake.
2. Aplicar la deduplicación mediante `event_id`.
3. Persistir el DataFrame utilizando `MEMORY_AND_DISK`.
4. Materializar el dataset mediante una operación `count()`.
5. Reutilizar el dataset persistido para las consultas analíticas Q1–Q5.

La deduplicación se realiza utilizando el identificador único del evento:

```python
df = df.dropDuplicates(["event_id"])
```

Posteriormente, el dataset se persiste:

```python
df = df.persist(StorageLevel.MEMORY_AND_DISK)
```

La materialización mediante `count()` permite ejecutar el procesamiento inicial y cargar el dataset en la caché antes de que las consultas analíticas sean ejecutadas.

Esto evita que cada consulta tenga que volver a leer todos los archivos Parquet y realizar nuevamente la deduplicación del historial.

---

### Optimización mediante caché

Inicialmente, las consultas analíticas necesitaban procesar el Datalake completo al ejecutarse, lo que generaba tiempos de respuesta elevados.

Se implementó la persistencia del DataFrame de eventos mediante Spark utilizando:

```python
StorageLevel.MEMORY_AND_DISK
```

El dataset se prepara durante el arranque del servicio y permanece persistido para ser reutilizado por las consultas.

La estrategia utilizada permite:

- Evitar múltiples lecturas del Datalake para cada consulta.
- Evitar repetir la deduplicación de eventos en cada ejecución.
- Reutilizar el DataFrame preparado por Spark.
- Reducir los tiempos de ejecución de Q1–Q5.
- Mantener la posibilidad de utilizar almacenamiento en disco si el dataset no cabe completamente en memoria.

La preparación inicial del dataset se realiza durante el startup de FastAPI, de manera que el servicio se inicializa con los datos históricos listos para ser utilizados por las consultas.

---

### Consultas analíticas implementadas

Se implementaron cinco consultas analíticas utilizando PySpark.

#### Q1 — Aprobaciones por fecha y modalidad

Analiza las tareas aprobadas durante los últimos 90 días con respecto al timestamp más reciente del dataset.

Los resultados se agrupan por fecha y modalidad, permitiendo observar la cantidad de tareas aprobadas en cada período.

La consulta utiliza eventos con estado `aprobada` y contabiliza las tareas mediante `task_id` único.

#### Q2 — Tasa de aprobación por equipo y mes

Analiza el desempeño de los equipos agrupando las tareas revisadas y aprobadas por equipo y mes.

La consulta relaciona las tareas que entraron en revisión con aquellas que posteriormente fueron aprobadas, permitiendo calcular la tasa de aprobación por equipo y período mensual.

#### Q3 — Duración de anotación por modalidad y tipo de tarea

Analiza la duración de las tareas desde su apertura hasta el momento en que fueron anotadas.

Los resultados se agrupan por modalidad y tipo de tarea e incluyen:

- Cantidad de tareas.
- Duración promedio de anotación.
- Percentil 95 (`p95`) de duración.

Para esta consulta se utilizan los eventos de estado `anotada`, donde se encuentra la duración correspondiente al proceso de anotación.

#### Q4 — Tasa de rechazo por cliente y proyecto

Analiza las tareas revisadas y rechazadas agrupándolas por cliente y proyecto.

La consulta contabiliza las tareas revisadas y las tareas rechazadas para calcular la tasa de rechazo correspondiente a cada combinación de cliente y proyecto.

#### Q5 — Cumplimiento de SLA por cliente

Analiza el tiempo transcurrido entre la apertura y la aprobación de cada tarea.

La consulta relaciona los eventos de apertura (`abierta`) con los eventos de aprobación (`aprobada`), calcula el tiempo transcurrido y lo compara con el SLA definido para cada tarea.

Los resultados se agrupan por cliente y contienen métricas de tiempo, incluyendo percentiles, así como el porcentaje de tareas que exceden su SLA.

---

### Endpoints implementados

El servicio Analítica expone los siguientes endpoints:

| Método | Endpoint | Descripción |
|---|---|---|
| `GET` | `/salud` | Verifica que el servicio esté disponible y que Spark pueda inicializarse. |
| `GET` | `/estadisticas` | Devuelve la cantidad de eventos únicos disponibles en el dataset analítico. |
| `GET` | `/q1` | Ejecuta la consulta analítica Q1. |
| `GET` | `/q2` | Ejecuta la consulta analítica Q2. |
| `GET` | `/q3` | Ejecuta la consulta analítica Q3. |
| `GET` | `/q4` | Ejecuta la consulta analítica Q4. |
| `GET` | `/q5` | Ejecuta la consulta analítica Q5. |
| `GET` | `/q1/plan` | Permite inspeccionar el plan de ejecución de Q1. |
| `GET` | `/q3/plan` | Permite inspeccionar el plan de ejecución de Q3. |
| `POST` | `/actualizar` | Limpia la caché del dataset y vuelve a leer, deduplicar y materializar los eventos del Datalake. |

---

### Cómo levantar el servicio Analítica

Desde la raíz del repositorio `proyecto2`, ejecutar:

```bash
docker compose -f docker-compose.analitica.yml up -d --build
```

El comando construye la imagen de Analítica e inicia el contenedor.

Para verificar que el contenedor está ejecutándose:

```bash
docker ps
```

El servicio debe aparecer como:

```text
analitica
```

Para observar los logs de inicialización:

```bash
docker logs -f analitica
```

Durante la inicialización se realizan las operaciones de lectura, deduplicación, persistencia y materialización del dataset.

Los logs permiten verificar que el dataset esté preparado para recibir consultas.

---

### Verificar el servicio

Consultar el endpoint de salud:

```bash
curl http://localhost:8004/salud
```

El endpoint verifica que el servicio esté disponible y que Spark haya sido inicializado.

---

### Consultar estadísticas del dataset

Para obtener la cantidad de eventos únicos cargados en Analítica:

```bash
curl http://localhost:8004/estadisticas
```

La respuesta contiene el total de eventos únicos disponibles en el DataFrame analítico.

El resultado obtenido durante las pruebas fue:

```json
{
  "eventos_unicos": 976087
}
```

Este resultado confirma que Spark pudo leer el Datalake y obtener el dataset deduplicado utilizado por las consultas analíticas.

---

### Ejecutar las consultas analíticas

Las consultas se pueden ejecutar individualmente mediante HTTP.

#### Q1

```bash
curl http://localhost:8004/q1
```

#### Q2

```bash
curl http://localhost:8004/q2
```

#### Q3

```bash
curl http://localhost:8004/q3
```

#### Q4

```bash
curl http://localhost:8004/q4
```

#### Q5

```bash
curl http://localhost:8004/q5
```

Cada endpoint devuelve:

- Identificador de la consulta.
- Cantidad de resultados obtenidos.
- Tiempo de ejecución de la consulta.
- Resultados analíticos en formato JSON.

El tiempo reportado corresponde a la ejecución de la consulta sobre el dataset preparado y persistido, no al tiempo completo de inicialización de Spark ni a la lectura inicial del Datalake.

---

### Inspección de planes de ejecución

El servicio permite consultar el plan de ejecución de algunas consultas para analizar cómo Spark procesa las operaciones.

Para Q1:

```bash
curl http://localhost:8004/q1/plan
```

Para Q3:

```bash
curl http://localhost:8004/q3/plan
```

Estos endpoints permiten inspeccionar el plan de ejecución de Spark, incluyendo las operaciones que componen el procesamiento de los DataFrames.

---

### Actualizar el dataset

Cuando se agregan nuevos eventos al Datalake y se desea que Analítica vuelva a procesarlos, se puede utilizar el endpoint:

```bash
curl -X POST http://localhost:8004/actualizar
```

El proceso realiza:

1. Limpieza de la caché anterior.
2. Liberación del DataFrame persistido.
3. Nueva lectura del Datalake.
4. Deduplicación por `event_id`.
5. Persistencia del nuevo dataset.
6. Materialización mediante `count()`.

Esto permite actualizar el dataset analítico sin necesidad de recrear manualmente el contenedor.

---

## Validación funcional de Analítica

Se implementó un script de pruebas controladas para verificar los resultados de las consultas Q1–Q5.

El script utiliza datos de prueba diseñados para comprobar diferentes escenarios, incluyendo cantidades de tareas, agrupaciones, duraciones, tasas y cumplimiento de SLA.

Las pruebas se ejecutan mediante:

```bash
python pruebas/analitica_controlada.py validar
```

La validación comprueba los resultados esperados de las cinco consultas.

| Consulta | Pruebas realizadas | Resultado |
|---|---:|---|
| Q1 | 3 | PASS |
| Q2 | 6 | PASS |
| Q3 | 6 | PASS |
| Q4 | 6 | PASS |
| Q5 | 7 | PASS |
| **Total** | **28** | **28/28 PASS** |

Se obtuvo un resultado de **28 pruebas exitosas y 0 fallidas**.

La validación permite verificar que las consultas producen los resultados esperados para los casos controlados antes de realizar las mediciones sobre el Datalake real.

La evidencia de esta validación se encuentra en:

```text
evidence/validacion_analitica.txt
```

---

## Benchmark de Analítica

Se realizaron pruebas de rendimiento utilizando el Datalake real de eventos almacenados en Parquet.

El benchmark mide:

- Tiempo de preparación del dataset.
- Tiempo de ejecución inicial de las consultas.
- Tiempo promedio de las consultas después de la preparación del dataset.
- Comparación del rendimiento utilizando diferentes cantidades de workers locales de Spark.

Las pruebas se realizaron sobre el Datalake existente, sin regenerar ni modificar los eventos almacenados.

### Dataset utilizado

| Característica | Valor |
|---|---:|
| Eventos únicos | 976,087 |
| Archivos Parquet | 2,249 |
| Tamaño aproximado | 91 MB |
| Formato | Parquet |
| Período de datos | Noviembre 2025 – Noviembre 2026 |

---

### Entorno de benchmark en WSL

Las pruebas experimentales se realizaron utilizando WSL Ubuntu, con Python, Java 17 y PySpark, permitiendo ejecutar Spark directamente en el entorno Linux.

#### Instalación de dependencias

Actualizar los paquetes del sistema:

```bash
sudo apt update
```

Instalar Python, herramientas de entornos virtuales y Java 17:

```bash
sudo apt install -y python3 python3-venv python3-pip openjdk-17-jdk
```

Verificar la versión de Java:

```bash
java -version
```

#### Configuración de Java 17

Definir la variable `JAVA_HOME` y actualizar el `PATH`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

#### Crear el entorno virtual de Python

Desde la raíz del repositorio:

```bash
python3 -m venv .venv
```

Activar el entorno virtual:

```bash
source .venv/bin/activate
```

Actualizar `pip`:

```bash
python -m pip install --upgrade pip
```

Instalar PySpark:

```bash
python -m pip install --upgrade pyspark
```

Verificar la versión instalada:

```bash
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
```

#### Configuración de variables de entorno

Desde la raíz del proyecto, con el entorno virtual activado:

```bash
source .venv/bin/activate
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_WORKERS=1
export DATA_LAKE="$(pwd)/datalake/eventos"
```

Las variables utilizadas son:

| Variable | Descripción |
|---|---|
| `JAVA_HOME` | Define la ubicación de la instalación de Java 17. |
| `PATH` | Permite ejecutar las herramientas de Java desde la terminal. |
| `SPARK_WORKERS` | Define la cantidad de workers locales de Spark utilizados en las pruebas. |
| `DATA_LAKE` | Define la ruta del Datalake de eventos que será procesado. |

Para realizar pruebas utilizando dos workers:

```bash
export SPARK_WORKERS=2
```

---

### Ejecución del benchmark

Para ejecutar las mediciones utilizando un worker local:

```bash
export SPARK_WORKERS=1
python pruebas/analitica_controlada.py benchmark --repeticiones 5
```

Para ejecutar las mediciones utilizando dos workers locales:

```bash
export SPARK_WORKERS=2
python pruebas/analitica_controlada.py benchmark --repeticiones 5
```

También se puede ejecutar la comparación automatizada de configuraciones:

```bash
python pruebas/analitica_controlada.py workers --repeticiones 5
```

Este comando ejecuta las pruebas utilizando `local[1]` y `local[2]`, permitiendo comparar el tiempo de preparación del dataset y el tiempo promedio de ejecución de las consultas Q1–Q5.

---

### Resultados de preparación del dataset

La preparación del dataset incluye la lectura de los archivos Parquet, deduplicación de eventos, persistencia y materialización del DataFrame.

| Configuración | Tiempo de preparación |
|---|---:|
| 1 worker (`local[1]`) | 227.3536 s |
| 2 workers (`local[2]`) | 134.7437 s |

Utilizar dos workers redujo el tiempo de preparación aproximadamente un **40.7%**.

---

### Comparación de tiempo promedio de consultas

Los siguientes resultados corresponden al promedio de cinco ejecuciones de cada consulta después de preparar el dataset.

| Consulta | 1 worker (s) | 2 workers (s) | Speedup | Reducción |
|---|---:|---:|---:|---:|
| Q1 | 1.3606 | 1.3322 | 1.02× | 2.1% |
| Q2 | 2.6840 | 2.0452 | 1.31× | 23.8% |
| Q3 | 1.3129 | 0.8164 | 1.61× | 37.8% |
| Q4 | 2.4142 | 1.5799 | 1.53× | 34.6% |
| Q5 | 6.6071 | 1.9414 | 3.40× | 70.6% |

---

### Comparación del tiempo total de consultas

Sumando los tiempos promedio de las cinco consultas:

| Configuración | Tiempo total Q1–Q5 |
|---|---:|
| 1 worker | 15.3790 s |
| 2 workers | 7.7150 s |

El uso de dos workers redujo el tiempo agregado de las consultas aproximadamente un **49.8%**, con un speedup agregado cercano a **1.99×**.

---

### Tiempo de preparación más consultas

Para comparar el tiempo de preparación del dataset junto con las cinco consultas analíticas:

| Configuración | Preparación | Consultas | Total |
|---|---:|---:|---:|
| 1 worker | 227.3536 s | 15.3790 s | 242.7326 s |
| 2 workers | 134.7437 s | 7.7150 s | 142.4587 s |

La configuración de dos workers redujo el tiempo total combinado aproximadamente un **41.3%**, representando un ahorro aproximado de **100.27 segundos** bajo las condiciones de prueba.

---

### Análisis de resultados

Los resultados muestran que la cantidad de workers locales afecta tanto la preparación del dataset como el tiempo de ejecución de las consultas analíticas.

- **Q1:** presentó una mejora pequeña, debido a que su tiempo de ejecución fue similar con ambas configuraciones.
- **Q2:** presentó una mejora moderada al utilizar dos workers.
- **Q3:** presentó una reducción importante del tiempo de ejecución.
- **Q4:** presentó una mejora significativa utilizando dos workers.
- **Q5:** fue la consulta con mayor mejora, reduciendo su tiempo promedio aproximadamente un 70.6%.

La configuración de dos workers obtuvo mejores resultados en la preparación del dataset y en el tiempo agregado de las consultas.

Para las pruebas experimentales realizadas en WSL, `local[2]` presentó el mejor rendimiento entre las configuraciones comparadas.

Estos resultados corresponden al entorno experimental utilizado y no representan una garantía de rendimiento en otros equipos o entornos de ejecución. La configuración del servicio Analítica en Docker se mantiene independiente de los resultados del benchmark local.

---

### Evidencia generada

Los resultados de las pruebas se almacenaron en los siguientes archivos:

| Archivo | Contenido |
|---|---|
| `evidence/validacion_analitica.txt` | Resultados de la validación funcional de Q1–Q5, con 28 pruebas exitosas. |
| `evidence/benchmark_analitica.txt` | Resultados detallados del benchmark y comparación de workers. |
| `evidence/resumen_analitica.txt` | Resumen de los resultados y conclusiones del benchmark. |

---

### Estado actual — Analítica

- Servicio FastAPI de Analítica implementado.
- Apache Spark integrado mediante PySpark.
- Configuración de Spark en modo local.
- Lectura del Datalake Parquet completada.
- Dataset histórico deduplicado mediante `event_id`.
- Dataset persistido utilizando `MEMORY_AND_DISK`.
- Materialización del dataset durante la inicialización del servicio.
- Endpoint `/salud` verificado.
- Endpoint `/estadisticas` verificado.
- 976,087 eventos únicos disponibles en el dataset analítico.
- Consulta Q1 implementada y validada.
- Consulta Q2 implementada y validada.
- Consulta Q3 implementada y validada.
- Consulta Q4 implementada y validada.
- Consulta Q5 implementada y validada.
- Endpoints de inspección de planes de ejecución implementados para Q1 y Q3.
- Endpoint de actualización del dataset implementado.
- Optimización mediante reutilización del DataFrame persistido.
- Validación funcional completada: 28/28 pruebas PASS.
- Benchmark ejecutado sobre el Datalake real.
- Comparación experimental realizada entre `local[1]` y `local[2]`.
- `local[2]` obtuvo mejores resultados en preparación del dataset y tiempo agregado de consultas bajo las condiciones de prueba.

