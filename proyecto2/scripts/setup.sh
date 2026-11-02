#!/usr/bin/env bash
set -euo pipefail

# Evita que Git Bash convierta rutas de Linux al llamar a docker
export MSYS_NO_PATHCONV=1

# Siempre se ejecuta desde la carpeta proyecto2
cd "$(dirname "$0")/.."

REG_SCRIPTS="servicios/registro/scripts"
URI="mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"

echo "==> Levantando los nodos de MongoDB"
docker compose up -d mongo1 mongo2 mongo3

echo "==> Esperando a que los tres nodos esten healthy"
for n in mongo1 mongo2 mongo3; do
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "$n" | tr -d '\r')" = "healthy" ]; do
    sleep 2
  done
done

echo "==> Inicializando el replica set (si hace falta)"
INIT=$(docker exec mongo1 mongosh --quiet --eval "try { rs.status().ok } catch (e) { 0 }" | tr -d '\r')
if [ "$INIT" != "1" ]; then
  docker exec mongo1 mongosh --quiet --eval "$(cat $REG_SCRIPTS/init-replica.js)"
else
  echo "    ya estaba inicializado"
fi

echo "==> Esperando al nodo primario"
until [ "$(docker exec mongo1 mongosh "$URI" --quiet --eval "db.hello().isWritablePrimary" 2>/dev/null | tr -d '\r')" = "true" ]; do
  sleep 2
done

echo "==> Creando base de datos y colecciones"
docker exec mongo1 mongosh "$URI" --quiet --eval "$(cat $REG_SCRIPTS/init-db.js)"

echo "==> Creando indices"
docker exec mongo1 mongosh "$URI" --quiet --eval "$(cat $REG_SCRIPTS/create-indexes.js)"

echo "Levantando el servicio de registro..."
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build registro

for i in $(seq 1 30); do
  if curl -fs http://127.0.0.1:8001/salud > /dev/null; then
    echo "registro responde en /salud"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "registro no respondio a tiempo"
    exit 1
  fi
  sleep 2
done


echo "==> Estado del replica set"
docker exec mongo1 mongosh "$URI" --quiet --eval "rs.status().members.map(m => m.name + ' ' + m.stateStr)"