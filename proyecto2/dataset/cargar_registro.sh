#!/usr/bin/env bash
set -euo pipefail

# Evita que Git Bash convierta rutas de Linux al llamar a docker
export MSYS_NO_PATHCONV=1

# Siempre se ejecuta desde la carpeta proyecto2
cd "$(dirname "$0")/.."

URI="mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
ORIGEN="datos/registro"
DESTINO="/tmp/registro"

if [ ! -f "$ORIGEN/tareas.jsonl" ]; then
  echo "No existen los archivos. Ejecuta primero python proyecto2/dataset/generar_registro.py"
  exit 1
fi

echo "==> Copiando archivos al contenedor"
docker exec mongo1 rm -rf "$DESTINO"
docker cp "$ORIGEN" mongo1:"$DESTINO"

echo "==> Vaciando colecciones (los indices se conservan)"
docker exec mongo1 mongosh "$URI" --quiet --eval "const d = db.getSiblingDB('plataforma'); ['clientes','proyectos','batches','tareas','anotadores'].forEach(c => d[c].deleteMany({}))"

for col in clientes proyectos batches tareas anotadores; do
  echo "==> Importando $col"
  docker exec mongo1 mongoimport --uri "$URI" --db plataforma --collection "$col" \
    --file "$DESTINO/$col.jsonl" --writeConcern majority --numInsertionWorkers 4
done

mkdir -p evidence
{
  echo "Carga del registro $(date -Iseconds)"
  docker exec mongo1 mongosh "$URI" --quiet --eval "const d = db.getSiblingDB('plataforma'); printjson({clientes: d.clientes.countDocuments(), proyectos: d.proyectos.countDocuments(), batches: d.batches.countDocuments(), tareas: d.tareas.countDocuments(), anotadores: d.anotadores.countDocuments(), tareas_disponibles: d.tareas.countDocuments({estado: 'disponible'})})"
} | tee evidence/dataset-registro-conteos.txt