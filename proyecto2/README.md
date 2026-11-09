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