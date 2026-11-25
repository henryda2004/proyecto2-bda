"""P4 y recorrido completo de una tarea LiDAR a traves del gateway.

Ejecuta, con el stack completo levantado (gateway, registro, sesion,
historial y analitica):

1. Crea un proyecto LiDAR con la taxonomia del enunciado y un lote.
2. Toma la tarea, guarda un borrador y la envia (HTTP 200).
3. Comprueba que un segundo envio responde HTTP 409.
4. Inicia la revision y la rechaza con un codigo de motivo.
   Verifica que en MongoDB queda "disponible" y en Parquet "rechazada".
5. Vuelve a tomar la tarea, la envia, la revisa y la aprueba.
6. Mide los segundos entre el HTTP 200 de la aprobacion y el momento
   en que la tarea es visible en Q1 de Spark.
7. Reprocesa el micro-lote (reenvia los mismos eventos al historial)
   y comprueba que todos se marcan como duplicados por event_id.
8. Guarda los planes de Q1 y Q3 y los puertos publicados.

Uso, desde proyecto2/:
    python pruebas/p4_recorrido_lidar.py

Salida: evidence/p4-recorrido-lidar-<fecha>.json, evidence/plan_q1.json,
evidence/plan_q3.json y evidence/p1-puertos.txt
"""

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

GATEWAY = "http://127.0.0.1:8000"
HISTORIAL = "http://127.0.0.1:8003"
ANALITICA = "http://127.0.0.1:8004"
RAIZ = Path(__file__).resolve().parents[1]
EVIDENCIA = RAIZ / "evidence"
COMPOSE = [
    "docker", "compose",
    "-f", "docker-compose.yml", "-f", "docker-compose.dev.yml",
    "-f", "docker-compose.gateway.yml", "-f", "docker-compose.historial.yml",
    "-f", "docker-compose.analitica.yml",
]
MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/plataforma?replicaSet=rs0"

pasos = []


def ahora():
    return datetime.now(timezone.utc).isoformat()


def http(metodo, url, datos=None, timeout=200):
    cuerpo = None if datos is None else json.dumps(datos).encode("utf-8")
    solicitud = Request(url, data=cuerpo, method=metodo,
                        headers={"Content-Type": "application/json"})
    inicio = time.perf_counter()
    try:
        respuesta = urlopen(solicitud, timeout=timeout)
    except HTTPError as error:
        respuesta = error
    with respuesta:
        contenido = respuesta.read()
        ms = round((time.perf_counter() - inicio) * 1000, 1)
        try:
            datos_resp = json.loads(contenido) if contenido else {}
        except ValueError:
            datos_resp = {"texto": contenido.decode("utf-8", "replace")[:500]}
        return respuesta.status, datos_resp, respuesta.headers.get("X-Request-ID"), ms


def paso(nombre, metodo, ruta, datos=None, esperado=None, base=GATEWAY, guardar=True):
    codigo, cuerpo, rid, ms = http(metodo, base + ruta, datos)
    visible = {k: v for k, v in cuerpo.items() if k != "token"} if isinstance(cuerpo, dict) else cuerpo
    registro = {
        "paso": nombre, "hora_utc": ahora(), "metodo": metodo, "ruta": ruta,
        "http": codigo, "esperado": esperado, "x_request_id": rid, "latencia_ms": ms,
    }
    if guardar:
        registro["respuesta"] = visible
    pasos.append(registro)
    marca = "OK " if esperado is None or codigo == esperado else "MAL"
    print(f"[{marca}] {nombre}: {metodo} {ruta} -> HTTP {codigo} ({ms} ms)", flush=True)
    if esperado is not None and codigo != esperado:
        guardar_evidencia(False, f"{nombre}: se esperaba {esperado} y llego {codigo}")
        sys.exit(1)
    return cuerpo


def mongo(js):
    salida = subprocess.run(
        ["docker", "exec", "mongo1", "mongosh", MONGO_URI, "--quiet", "--eval", js],
        capture_output=True, text=True, timeout=60,
    )
    texto = salida.stdout.strip().splitlines()
    return json.loads(texto[-1]) if texto else None


def estado_mongo(tarea_id):
    return mongo(
        "const t=db.tareas.findOne({_id:'%s'},{estado:1,version:1,ultimo_evento_id:1});"
        "print(JSON.stringify(t))" % tarea_id
    )


def filas_parquet(tarea_id):
    codigo = (
        "import json,pyarrow.dataset as ds;"
        "t=ds.dataset('/datalake/eventos',format='parquet',partitioning='hive')"
        ".to_table(filter=ds.field('tarea_id')=='%s',"
        "columns=['event_id','estado','version','motivo_rechazo','event_ts']).to_pylist();"
        "print(json.dumps(sorted(t,key=lambda f:f['version']),default=str))" % tarea_id
    )
    salida = subprocess.run(["docker", "exec", "historial", "python", "-c", codigo],
                            capture_output=True, text=True, timeout=120)
    if salida.returncode != 0:
        return {"error": salida.stderr.strip()[-500:]}
    return json.loads(salida.stdout.strip().splitlines()[-1])


def flush():
    return paso("flush del historial", "POST", "/flush", {}, 200, base=HISTORIAL)


def guardar_evidencia(superada, detalle, extra=None):
    marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    salida = EVIDENCIA / f"p4-recorrido-lidar-{marca}.json"
    documento = {
        "fecha_hora_utc": ahora(),
        "objetivo": "Recorrido completo de una tarea LiDAR por el gateway; fila rechazada en "
                    "Parquet y disponible en MongoDB; segundos entre HTTP 200 y visibilidad en "
                    "Q1; reproceso de micro-lote y deduplicacion por event_id.",
        "comando": "python pruebas/p4_recorrido_lidar.py",
        "prueba_superada": superada,
        "detalle": detalle,
        **(extra or {}),
        "pasos": pasos,
    }
    salida.write_text(json.dumps(documento, ensure_ascii=False, indent=2, default=str),
                      encoding="utf-8")
    print(f"\nEvidencia guardada en {salida}")


def main():
    EVIDENCIA.mkdir(exist_ok=True)
    resultado = {}

    # Datos reales del catalogo cargado en MongoDB.
    catalogo = mongo(
        "print(JSON.stringify({c:db.clientes.findOne({},{_id:1})._id,"
        "a:db.anotadores.findOne({es_revisor:false},{_id:1})._id,"
        "r:db.anotadores.findOne({es_revisor:true},{_id:1})._id}))"
    )
    if not catalogo:
        print("No se encontro el catalogo en MongoDB. Cargue el dataset del registro primero.")
        sys.exit(1)
    cliente, anotador, revisor = catalogo["c"], catalogo["a"], catalogo["r"]
    resultado["catalogo_usado"] = catalogo

    # 1. Proyecto LiDAR y lote.
    proyecto = paso("crear proyecto LiDAR", "POST", "/proyectos", {
        "cliente_id": cliente,
        "nombre": "Demo LiDAR vehiculos autonomos",
        "modalidad": "lidar",
        "sla_horas": 48,
        "reglas": "Un skater se clasifica como ciclista y no como peaton.",
        "taxonomia": [
            {"nombre": "lidar#peaton", "instruccion": "Personas a pie"},
            {"nombre": "lidar#vehiculo", "instruccion": "Autos, buses y camiones"},
            {"nombre": "lidar#ciclista", "instruccion": "Bicicletas y skaters"},
            {"nombre": "lidar#signal", "instruccion": "Senales de transito"},
        ],
    }, 201)["id"]
    nube = b"demo-lidar-frame-000123"
    paso("registrar lote", "POST", f"/proyectos/{proyecto}/lotes", {
        "nombre": "Lote LiDAR demo",
        "tareas": [{
            "uri": "s3://lidar-demo/frame-000123.pcd",
            "sha256": sha256(nube).hexdigest(),
            "bytes": 48213760,
            "mime": "application/octet-stream",
            "meta": {"puntos": 1204000, "x_min": -80.0, "x_max": 80.0,
                     "z_min": -2.5, "z_max": 6.0},
            "tipo_tarea": "cuboide_3d",
        }],
    }, 201)

    anotacion = {"cuboides": [{"etiqueta": "lidar#ciclista",
                               "centro": [12.4, -3.1, 0.9], "dimensiones": [1.8, 0.6, 1.7],
                               "yaw": 0.35}]}

    # 2. Tomar, borrador y enviar.
    asignada = paso("siguiente tarea", "POST", "/tareas/siguiente",
                    {"proyecto_id": proyecto, "anotador_id": anotador}, 200)
    tarea = asignada["tarea"]["id"]
    resultado["tarea_id"] = tarea
    propietario = {"anotador_id": anotador, "token": asignada["token"]}
    paso("guardar borrador", "PUT", f"/tareas/{tarea}/borrador",
         {**propietario, "borrador": anotacion}, 200)
    paso("enviar anotacion", "POST", f"/tareas/{tarea}/enviar",
         {**propietario, "anotacion": anotacion}, 200)

    # 3. Segundo envio.
    paso("segundo envio (debe ser 409)", "POST", f"/tareas/{tarea}/enviar",
         {**propietario, "anotacion": anotacion}, 409)

    # 4. Revision con rechazo.
    paso("iniciar revision", "POST", f"/tareas/{tarea}/revision",
         {"accion": "iniciar_revision", "revisor_id": revisor}, 200)
    paso("rechazar con motivo", "POST", f"/tareas/{tarea}/revision",
         {"accion": "rechazar", "revisor_id": revisor, "motivo": "ETIQUETA_INCORRECTA"}, 200)
    time.sleep(2)
    flush()
    resultado["tras_rechazo"] = {
        "mongodb": estado_mongo(tarea),
        "parquet": filas_parquet(tarea),
    }
    print("  MongoDB tras rechazo:", resultado["tras_rechazo"]["mongodb"])
    print("  Parquet tras rechazo:", [f["estado"] for f in resultado["tras_rechazo"]["parquet"]]
          if isinstance(resultado["tras_rechazo"]["parquet"], list) else resultado["tras_rechazo"]["parquet"])

    # 5. Retrabajo y aprobacion.
    asignada = paso("siguiente tarea (retrabajo)", "POST", "/tareas/siguiente",
                    {"proyecto_id": proyecto, "anotador_id": anotador}, 200)
    if asignada["tarea"]["id"] != tarea:
        print("  Aviso: el retrabajo entrego otra tarea")
    propietario = {"anotador_id": anotador, "token": asignada["token"]}
    paso("enviar corregida", "POST", f"/tareas/{tarea}/enviar",
         {**propietario, "anotacion": anotacion}, 200)
    paso("iniciar revision 2", "POST", f"/tareas/{tarea}/revision",
         {"accion": "iniciar_revision", "revisor_id": revisor}, 200)
    paso("aprobar", "POST", f"/tareas/{tarea}/revision",
         {"accion": "aprobar", "revisor_id": revisor}, 200)
    t200 = time.perf_counter()
    hora_200 = ahora()
    exportada = paso("exportar aprobadas", "GET", f"/proyectos/{proyecto}/exportar?limite=10",
                     None, 200)

    # 6. Frescura: HTTP 200 -> visible en Q1.
    hoy = datetime.now(timezone.utc).date().isoformat()
    time.sleep(2)
    flush()
    paso("actualizar dataset Spark", "POST", "/actualizar", {}, 200, base=ANALITICA)
    visible_en = None
    for intento in range(10):
        q1 = paso(f"Q1 por gateway (intento {intento + 1})", "GET", "/analitica/q1",
                  None, 200, guardar=False)
        filas = [f for f in q1.get("resultados", [])
                 if str(f.get("dia")) == hoy and f.get("modalidad") == "lidar"]
        if filas:
            visible_en = round(time.perf_counter() - t200, 2)
            resultado["q1_fila_lidar_hoy"] = filas
            break
        time.sleep(3)
    resultado["frescura"] = {
        "hora_http_200_aprobacion": hora_200,
        "segundos_hasta_visible_en_q1": visible_en,
        "mecanismo": "flush del historial + POST /actualizar en analitica + GET /analitica/q1",
    }
    print(f"  Segundos entre HTTP 200 y visible en Q1: {visible_en}")

    # Estado final en ambos almacenamientos.
    resultado["final"] = {"mongodb": estado_mongo(tarea), "parquet": filas_parquet(tarea)}

    # 7. Reproceso del micro-lote: reenviar los mismos eventos.
    eventos = mongo(
        "print(JSON.stringify(db.tareas.findOne({_id:'%s'},{_id:1}) ? 1 : 0))" % tarea
    )
    filas = resultado["final"]["parquet"] if isinstance(resultado["final"]["parquet"], list) else []
    antes = paso("estadisticas historial antes", "GET", "/estadisticas", None, 200, base=HISTORIAL)
    reenviados = []
    for e in pasos:
        r = e.get("respuesta")
        if isinstance(r, dict) and isinstance(r.get("evento"), dict):
            reenviados.append(r["evento"])
    respuesta_lote = paso("reprocesar micro-lote (mismos eventos)", "POST", "/eventos/lote",
                          reenviados, 200, base=HISTORIAL)
    despues = paso("estadisticas historial despues", "GET", "/estadisticas", None, 200,
                   base=HISTORIAL)
    resultado["reproceso"] = {
        "eventos_reenviados": len(reenviados),
        "respuesta": respuesta_lote,
        "total_antes": antes.get("total"),
        "total_despues": despues.get("total"),
        "duplicados_descartados": (antes.get("total") == despues.get("total")),
        "filas_parquet_de_la_tarea": len(filas),
        "event_id_unicos_en_parquet": len({f["event_id"] for f in filas}),
    }
    print("  Reproceso:", {k: v for k, v in resultado["reproceso"].items() if k != "respuesta"})

    # 8. Planes y puertos.
    for q in ("q1", "q3"):
        codigo, plan, _, _ = http("GET", f"{ANALITICA}/{q}/plan")
        (EVIDENCIA / f"plan_{q}.json").write_text(
            json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  Plan de {q.upper()} guardado (HTTP {codigo})")
    puertos = subprocess.run(COMPOSE + ["ps", "--format",
                                        "table {{.Name}}\t{{.Ports}}"],
                             capture_output=True, text=True, cwd=RAIZ)
    (EVIDENCIA / "p1-puertos.txt").write_text(
        f"P1 puertos publicados {ahora()}\n" + puertos.stdout + puertos.stderr, encoding="utf-8")
    print("  Puertos guardados en evidence/p1-puertos.txt")

    # Verificaciones.
    rech = resultado["tras_rechazo"]
    estados_rech = [f["estado"] for f in rech["parquet"]] if isinstance(rech["parquet"], list) else []
    estados_fin = [f["estado"] for f in filas]
    checks = {
        "mongo_disponible_tras_rechazo": (rech["mongodb"] or {}).get("estado") == "disponible",
        "parquet_tiene_rechazada": "rechazada" in estados_rech,
        "mongo_aprobada_al_final": (resultado["final"]["mongodb"] or {}).get("estado") == "aprobada",
        "parquet_tiene_aprobada": "aprobada" in estados_fin,
        "exportada": any(t.get("id") == tarea or t.get("tarea_id") == tarea
                         for t in exportada.get("tareas", exportada.get("items", []))),
        "visible_en_q1": visible_en is not None,
        "reproceso_sin_duplicados": resultado["reproceso"]["duplicados_descartados"],
        "event_id_unicos": len(filas) == len({f["event_id"] for f in filas}),
    }
    resultado["verificaciones"] = checks
    for k, v in checks.items():
        print(f"  {'OK ' if v else 'MAL'} {k}")
    guardar_evidencia(all(checks.values()), "ver verificaciones", resultado)


if __name__ == "__main__":
    try:
        main()
    except (URLError, OSError) as error:
        print("No se pudo contactar un servicio:", error)
        guardar_evidencia(False, f"error de conexion: {error}")
        sys.exit(1)
