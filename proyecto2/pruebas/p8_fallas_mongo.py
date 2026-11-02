"""P8. Fallas de MongoDB.

Escenarios
  A. cae un nodo secundario (se debe seguir escribiendo)
  B. cae el nodo primario (hay eleccion de un nuevo primario)
  C. caen los dos secundarios (sin mayoria no se debe confirmar ninguna escritura)

En cada uno un escritor continuo pide tareas con POST /tareas/siguiente y anota el
codigo HTTP de cada intento. Cada escenario usa su propio proyecto con tareas de sobra.
Al final se comprueba que toda tarea confirmada con HTTP 200 sigue en MongoDB (RPO)
y se mide la pausa mas larga entre dos respuestas 200 (RTO).
Los nodos se detienen con docker stop -t 0 (caida abrupta) y se restauran con docker start.
"""
import argparse
import hashlib
import json
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

NODOS = ["mongo1", "mongo2", "mongo3"]
URI_RS = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
TAREAS = 600
LOTES = 2
JS_ESTADO = (
    "print(JSON.stringify(rs.status().members.map("
    "m => ({nombre: m.name.split(':')[0], estado: m.stateStr}))))"
)


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def mongosh(nodo, uri, js):
    r = docker("exec", nodo, "mongosh", uri, "--quiet", "--eval", js)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout).strip())
    return r.stdout.strip()


def estado_rs():
    for nodo in NODOS:
        try:
            uri = f"mongodb://{nodo}:27017/?directConnection=true"
            return json.loads(mongosh(nodo, uri, JS_ESTADO))
        except Exception:
            continue
    return []


def sano(miembros):
    estados = [m["estado"] for m in miembros]
    return (
        len(estados) == 3
        and estados.count("PRIMARY") == 1
        and estados.count("SECONDARY") == 2
    )


def esperar_sano(limite=150):
    t0 = time.time()
    while time.time() - t0 < limite:
        if sano(estado_rs()):
            return round(time.time() - t0, 2)
        time.sleep(2)
    raise RuntimeError("El replica set no volvio a un estado sano")


def ids_abiertas(proyecto_id):
    js = (
        "print(JSON.stringify(db.getSiblingDB('plataforma').tareas.find("
        "{proyecto_id: '" + proyecto_id + "', estado: 'abierta'}, {_id: 1})"
        ".toArray().map(x => x._id)))"
    )
    return set(json.loads(mongosh("mongo1", URI_RS, js)))


def http_post(url, cuerpo, timeout=15):
    datos = json.dumps(cuerpo).encode()
    req = urllib.request.Request(
        url, data=datos, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            texto = r.read().decode()
            return r.status, (json.loads(texto) if texto else None)
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception:
        return 0, None


def percentil(valores, p):
    if not valores:
        return None
    ordenados = sorted(valores)
    idx = max(0, -(-len(ordenados) * p // 100) - 1)
    return round(ordenados[int(idx)], 3)


class Escritor(threading.Thread):
    def __init__(self, url, proyecto_id, anotador):
        super().__init__(daemon=True)
        self.url = url
        self.proyecto_id = proyecto_id
        self.anotador = anotador
        self.parar = threading.Event()
        self.registro = []

    def run(self):
        while not self.parar.is_set():
            t0 = time.time()
            estado, cuerpo = http_post(
                f"{self.url}/tareas/siguiente",
                {
                    "anotador_id": self.anotador,
                    "proyecto_id": self.proyecto_id,
                    "plazo_seg": 3600,
                },
            )
            self.registro.append(
                {
                    "t": t0,
                    "http": estado,
                    "tarea": cuerpo["tarea"]["id"] if cuerpo else None,
                    "ms": (time.time() - t0) * 1000,
                }
            )
            time.sleep(0.1)


def ahora_iso():
    return datetime.now(timezone.utc).isoformat()


def preparar_proyecto(args, etiqueta):
    estado, proyecto = http_post(
        f"{args.url}/proyectos",
        {
            "cliente_id": "cli-p8",
            "nombre": f"Prueba P8 {etiqueta}",
            "modalidad": "lidar",
            "taxonomia": [{"nombre": "lidar#peaton", "instruccion": "Persona a pie"}],
            "sla_horas": 48,
        },
    )
    if estado != 201:
        raise SystemExit(f"No se pudo crear el proyecto, HTTP {estado}")
    proyecto_id = proyecto["id"]
    for lote in range(LOTES):
        tareas = [
            {
                "uri": f"s3://p8/{etiqueta}/escena-{lote}-{i}.pcd",
                "sha256": hashlib.sha256(f"p8-{etiqueta}-{lote}-{i}".encode()).hexdigest(),
                "bytes": 1048576,
                "mime": "application/octet-stream",
                "meta": {},
                "tipo_tarea": "cuboide_3d",
                "prioridad": 0,
            }
            for i in range(TAREAS)
        ]
        estado, _ = http_post(
            f"{args.url}/proyectos/{proyecto_id}/lotes",
            {"nombre": f"lote-p8-{lote}", "tareas": tareas},
            timeout=60,
        )
        if estado != 201:
            raise SystemExit(f"No se pudo crear el lote, HTTP {estado}")
    return proyecto_id


def correr_escenario(nombre, descripcion, elegir, ventana, args, proyecto_id):
    antes = estado_rs()
    primario = next(m["nombre"] for m in antes if m["estado"] == "PRIMARY")
    secundarios = [m["nombre"] for m in antes if m["estado"] == "SECONDARY"]
    nodos = elegir(primario, secundarios)
    abiertas_antes = ids_abiertas(proyecto_id)

    escritor = Escritor(args.url, proyecto_id, "p8-ana-001")
    acciones = []
    escritor.start()
    try:
        time.sleep(args.base)
        for n in nodos:
            docker("stop", "-t", "0", n)
            acciones.append({"hora_utc": ahora_iso(), "accion": f"docker stop -t 0 {n}"})
        t_falla = time.time()
        time.sleep(ventana)
        durante = estado_rs()

        t_restaurar = time.time()
        for n in nodos:
            docker("start", n)
            acciones.append({"hora_utc": ahora_iso(), "accion": f"docker start {n}"})
        segundos_sano = esperar_sano()
        time.sleep(args.base)
    finally:
        escritor.parar.set()
        escritor.join()
        for n in NODOS:
            docker("start", n)

    despues = estado_rs()
    reg = escritor.registro
    ok = [r for r in reg if r["http"] == 200]

    def fin(r):
        return r["t"] + r["ms"] / 1000

    en_ventana = [r for r in reg if t_falla <= r["t"] < t_restaurar]
    ventana_ok = sum(1 for r in en_ventana if r["http"] == 200)
    ventana_503 = sum(1 for r in en_ventana if r["http"] == 503)
    ventana_otros = len(en_ventana) - ventana_ok - ventana_503
    codigos = dict(Counter(str(r["http"]) for r in en_ventana))

    pausas = [fin(ok[i + 1]) - fin(ok[i]) for i in range(len(ok) - 1)]
    pausa_tipica = percentil(pausas, 50)
    rto = round(max(pausas), 2) if pausas else None

    tras_restaurar = [r for r in ok if r["t"] >= t_restaurar]
    reanuda = bool(tras_restaurar)
    recuperacion = round(fin(tras_restaurar[0]) - t_restaurar, 2) if tras_restaurar else None

    primarios_durante = [m["nombre"] for m in durante if m["estado"] == "PRIMARY"]
    nuevo_primario = bool(primarios_durante) and primarios_durante[0] != primario

    confirmadas = [r["tarea"] for r in ok]
    abiertas = ids_abiertas(proyecto_id)
    perdidas = sorted(set(confirmadas) - abiertas)
    sin_confirmar = sorted((abiertas - abiertas_antes) - set(confirmadas))
    duplicadas = len(confirmadas) - len(set(confirmadas))
    latencias = [r["ms"] for r in ok]
    agotadas = sum(1 for r in reg if r["http"] == 204)

    if nombre.startswith("A"):
        superado = ventana_ok > 0 and not perdidas and duplicadas == 0
    elif nombre.startswith("B"):
        superado = (
            nuevo_primario
            and ventana_ok > 0
            and reanuda
            and not perdidas
            and duplicadas == 0
        )
    else:
        superado = ventana_ok == 0 and ventana_503 > 0 and reanuda and not perdidas
    superado = superado and agotadas == 0

    return {
        "escenario": nombre,
        "descripcion": descripcion,
        "roles_antes": antes,
        "nodos_detenidos": nodos,
        "acciones": acciones,
        "roles_durante_la_falla": durante,
        "roles_despues": despues,
        "ventana_falla_s": ventana,
        "segundos_hasta_replica_set_sano": segundos_sano,
        "nuevo_primario_elegido": nuevo_primario,
        "durante_la_falla": {
            "intentos": len(en_ventana),
            "http_200": ventana_ok,
            "http_503": ventana_503,
            "otros": ventana_otros,
            "codigos": codigos,
        },
        "rto_pausa_maxima_entre_200_s": rto,
        "pausa_tipica_entre_200_s": pausa_tipica,
        "recuperacion_tras_restaurar_s": recuperacion,
        "intentos_sin_tareas_http_204": agotadas,
        "rpo": {
            "confirmadas_http_200": len(confirmadas),
            "confirmadas_ausentes_en_mongo": len(perdidas),
            "duplicadas": duplicadas,
            "abiertas_en_mongo_sin_respuesta_200": len(sin_confirmar),
        },
        "latencia_ms_http_200": {
            "muestras": len(latencias),
            "p50": percentil(latencias, 50),
            "p95": percentil(latencias, 95),
            "p99": percentil(latencias, 99),
            "max": round(max(latencias), 3) if latencias else None,
        },
        "superado": superado,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8001")
    ap.add_argument("--base", type=int, default=5)
    args = ap.parse_args()
    inicio = datetime.now(timezone.utc)

    if not sano(estado_rs()):
        raise SystemExit("El replica set no esta sano. Ejecuta primero proyecto2/scripts/setup.sh")

    mongosh(
        "mongo1",
        URI_RS,
        "const d = db.getSiblingDB('plataforma');"
        "d.clientes.updateOne({_id:'cli-p8'},{$set:{nombre:'Cliente P8'}},{upsert:true});"
        "d.anotadores.updateOne({_id:'p8-ana-001'},{$set:{nombre:'p8-ana-001',equipo_id:'equipo-01',es_revisor:false}},{upsert:true});"
        "print('ok')",
    )

    escenarios = [
        ("A_cae_un_secundario", "Se detiene un nodo secundario", lambda p, s: [s[0]], 15),
        ("B_cae_el_primario", "Se detiene el nodo primario y se elige otro", lambda p, s: [p], 40),
        ("C_sin_mayoria", "Se detienen los dos secundarios, queda un solo nodo", lambda p, s: list(s), 25),
    ]
    resultados = []
    for nombre, descripcion, elegir, ventana in escenarios:
        print("Corriendo", nombre, "...")
        proyecto_id = preparar_proyecto(args, nombre)
        resultados.append(
            correr_escenario(nombre, descripcion, elegir, ventana, args, proyecto_id)
        )

    evidencia = {
        "fecha_hora_utc": inicio.isoformat(),
        "objetivo": "Comprobar el comportamiento del registro ante la caida de nodos de MongoDB, medir el RTO y verificar que lo confirmado con HTTP 200 no se pierde (RPO)",
        "configuracion": {
            "url": args.url,
            "write_concern": "majority con wTimeoutMS 3000",
            "escritura": "POST /tareas/siguiente cada 0.1 s",
            "falla": "docker stop -t 0",
            "segundos_base": args.base,
            "tareas_por_escenario": TAREAS * LOTES,
            "definicion_rto": "pausa maxima entre dos respuestas HTTP 200 consecutivas",
        },
        "comando": "python proyecto2/pruebas/p8_fallas_mongo.py",
        "escenarios": resultados,
        "prueba_superada": all(e["superado"] for e in resultados),
    }
    destino = Path(__file__).resolve().parent.parent / "evidence" / "p8-fallas-mongo.json"
    destino.write_text(json.dumps(evidencia, indent=2), encoding="utf-8")

    for e in resultados:
        d = e["durante_la_falla"]
        print(
            f"{e['escenario']:22s} 200 {d['http_200']:4d} 503 {d['http_503']:4d} "
            f"codigos {d['codigos']} pausa max {e['rto_pausa_maxima_entre_200_s']} s "
            f"(tipica {e['pausa_tipica_entre_200_s']} s) "
            f"nuevo primario {e['nuevo_primario_elegido']} "
            f"204 {e['intentos_sin_tareas_http_204']} "
            f"perdidas {e['rpo']['confirmadas_ausentes_en_mongo']} superado {e['superado']}"
        )
    print("Prueba superada", evidencia["prueba_superada"])
    print("Evidencia guardada en", destino)
    raise SystemExit(0 if evidencia["prueba_superada"] else 1)


if __name__ == "__main__":
    main()