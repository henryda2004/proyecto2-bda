"""P2. Registro y concurrencia.

Crea un proyecto con un lote de 10 tareas y lanza 50 solicitudes simultaneas
de POST /tareas/siguiente. Debe haber 10 asignaciones y 40 respuestas 204,
y ninguna tarea asignada a dos personas.
"""
import argparse
import hashlib
import json
import subprocess
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
SOLICITUDES = 50
TAREAS_LIBRES = 10


def mongosh(js):
    r = subprocess.run(
        ["docker", "exec", "mongo1", "mongosh", MONGO_URI, "--quiet", "--eval", js],
        capture_output=True,
        text=True,
        check=True,
    )
    return r.stdout.strip()


def http_post(url, cuerpo):
    datos = json.dumps(cuerpo).encode()
    req = urllib.request.Request(
        url, data=datos, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            texto = r.read().decode()
            return r.status, (json.loads(texto) if texto else None)
    except urllib.error.HTTPError as e:
        return e.code, None


def pedir(url, anotador, proyecto_id, barrera):
    barrera.wait()
    t0 = time.perf_counter()
    estado, cuerpo = http_post(
        f"{url}/tareas/siguiente",
        {"anotador_id": anotador, "proyecto_id": proyecto_id},
    )
    return {
        "anotador": anotador,
        "http": estado,
        "tarea": cuerpo["tarea"]["id"] if cuerpo else None,
        "ms": (time.perf_counter() - t0) * 1000,
    }


def percentil(valores, p):
    ordenados = sorted(valores)
    idx = max(0, -(-len(ordenados) * p // 100) - 1)
    return round(ordenados[int(idx)], 3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8001")
    args = ap.parse_args()
    inicio = datetime.now(timezone.utc)

    anotadores = [f"p2-ana-{i:03d}" for i in range(1, SOLICITUDES + 1)]
    mongosh(
        "const d = db.getSiblingDB('plataforma');"
        "d.clientes.updateOne({_id:'cli-p2'},{$set:{nombre:'Cliente P2'}},{upsert:true});"
        "for (let i = 1; i <= " + str(SOLICITUDES) + "; i++) {"
        "const id = 'p2-ana-' + String(i).padStart(3,'0');"
        "d.anotadores.updateOne({_id:id},{$set:{nombre:id,equipo_id:'eq-01',es_revisor:false}},{upsert:true});"
        "}"
    )

    replicas = json.loads(
        mongosh(
            "print(JSON.stringify(rs.status().members.map(m => ({nombre: m.name, estado: m.stateStr}))))"
        )
    )

    estado, proyecto = http_post(
        f"{args.url}/proyectos",
        {
            "cliente_id": "cli-p2",
            "nombre": "Prueba P2 concurrencia",
            "modalidad": "lidar",
            "taxonomia": [{"nombre": "lidar#peaton", "instruccion": "Persona a pie"}],
            "sla_horas": 48,
        },
    )
    if estado != 201:
        raise SystemExit(f"No se pudo crear el proyecto, HTTP {estado}")
    proyecto_id = proyecto["id"]

    tareas = [
        {
            "uri": f"s3://p2/escena-{i}.pcd",
            "sha256": hashlib.sha256(str(i).encode()).hexdigest(),
            "bytes": 1048576,
            "mime": "application/octet-stream",
            "meta": {"min": -50.0, "max": 120.5},
            "tipo_tarea": "cuboide_3d",
            "prioridad": i,
        }
        for i in range(1, TAREAS_LIBRES + 1)
    ]
    estado, _ = http_post(
        f"{args.url}/proyectos/{proyecto_id}/lotes",
        {"nombre": "lote-p2", "tareas": tareas},
    )
    if estado != 201:
        raise SystemExit(f"No se pudo crear el lote, HTTP {estado}")

    barrera = threading.Barrier(SOLICITUDES)
    with ThreadPoolExecutor(max_workers=SOLICITUDES) as pool:
        futuros = [
            pool.submit(pedir, args.url, a, proyecto_id, barrera) for a in anotadores
        ]
        resultados = [f.result() for f in futuros]

    asignadas = [r for r in resultados if r["http"] == 200]
    sin_tarea = [r for r in resultados if r["http"] == 204]
    otras = [r for r in resultados if r["http"] not in (200, 204)]
    ids = [r["tarea"] for r in asignadas]
    duplicadas = len(ids) - len(set(ids))

    en_mongo = json.loads(
        mongosh(
            "print(JSON.stringify(db.getSiblingDB('plataforma').tareas.aggregate(["
            "{$match:{proyecto_id:'" + proyecto_id + "'}},"
            "{$group:{_id:'$estado',n:{$sum:1}}}]).toArray()))"
        )
    )
    abiertas_mongo = sum(x["n"] for x in en_mongo if x["_id"] == "abierta")

    ok = (
        len(asignadas) == TAREAS_LIBRES
        and len(sin_tarea) == SOLICITUDES - TAREAS_LIBRES
        and not otras
        and duplicadas == 0
        and abiertas_mongo == TAREAS_LIBRES
    )

    latencias = [r["ms"] for r in resultados]
    evidencia = {
        "fecha_hora_utc": inicio.isoformat(),
        "objetivo": "Verificar que no hay doble asignacion con 50 solicitudes simultaneas y 10 tareas libres",
        "configuracion": {
            "url": args.url,
            "solicitudes": SOLICITUDES,
            "tareas_libres": TAREAS_LIBRES,
            "write_concern": "majority",
        },
        "comando": "python proyecto2/pruebas/p2_concurrencia.py",
        "estado_replica_set": replicas,
        "resultado": {
            "asignaciones_200": len(asignadas),
            "sin_tarea_204": len(sin_tarea),
            "otras_respuestas": len(otras),
            "tareas_duplicadas": duplicadas,
            "tareas_abiertas_en_mongo": abiertas_mongo,
            "estados_en_mongo": en_mongo,
            "prueba_superada": ok,
        },
        "latencia_ms": {
            "muestras": len(latencias),
            "p50": percentil(latencias, 50),
            "p95": percentil(latencias, 95),
            "p99": percentil(latencias, 99),
        },
        "detalle": resultados,
    }

    destino = Path(__file__).resolve().parent.parent / "evidence" / "p2-concurrencia.json"
    destino.write_text(json.dumps(evidencia, indent=2), encoding="utf-8")

    print(json.dumps({k: evidencia[k] for k in ("estado_replica_set", "resultado", "latencia_ms")}, indent=2))
    print("Evidencia guardada en", destino)
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()