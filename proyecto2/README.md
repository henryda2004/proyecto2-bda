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