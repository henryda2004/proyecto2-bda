# Datos del proyecto

Esta carpeta contiene los datos generados para el registro (`datos/registro/*.jsonl`, se crean al ejecutar el generador). Los generadores viven en `dataset/` y en `servicios/historial/generador/`. Todos los comandos se ejecutan desde la carpeta `proyecto2/`.

## 1. Registro operacional (MongoDB)

Volumen: 60 clientes, 120 proyectos activos (más 60 cerrados que solo existen en el historial), 600 lotes, 60 000 tareas (30 % texto, 30 % imagen, 20 % video, 20 % LiDAR) y 300 anotadores en 30 equipos, con un revisor por equipo (10 %).

```bash
python dataset/generar_registro.py          # escribe datos/registro/*.jsonl
bash dataset/cargar_registro.sh             # carga en MongoDB y escribe evidence/dataset-registro-conteos.txt
```

En Windows use Git Bash para el `.sh`: `& "C:\Program Files\Git\bin\bash.exe" dataset/cargar_registro.sh`.

## 2. Historial (Parquet)

Volumen: 100 000 tareas sintéticas y aproximadamente 800 000 eventos en doce meses. El generador envía lotes HTTP al servicio historial (puerto 8003, solo localhost), que deduplica por `event_id` y escribe micro-lotes Parquet en `datalake/eventos/mes=AAAA-MM/`.

```bash
python servicios/historial/generador/generar_historial.py --tareas 100000 --batch-size 1000 --seed 5126
curl -X POST http://127.0.0.1:8003/flush
docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.gateway.yml -f docker-compose.historial.yml -f docker-compose.analitica.yml restart analitica
```

La carga completa tarda varios minutos y depende de la máquina. Si las pruebas de carga agotan las tareas disponibles, se vuelve a ejecutar el generador del registro y se anota la fecha, el motivo y el efecto sobre la comparabilidad.

## 3. Reinicio desde cero

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.gateway.yml -f docker-compose.historial.yml -f docker-compose.analitica.yml down -v
rm -rf datalake/eventos/mes=*
bash scripts/setup.sh
```
