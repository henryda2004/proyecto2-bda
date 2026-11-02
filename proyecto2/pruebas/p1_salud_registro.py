"""P1 (parte del registro). /salud en estado normal, degradado y caido.

Normal: tres nodos sanos. Degradado: se detiene un secundario. Caido: se detienen
los dos secundarios (sin mayoria). Se restaura el replica set al terminar.
Salida: proyecto2/evidence/p1-salud-registro.json
"""
import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from p8_fallas_mongo import ahora_iso, docker, esperar_sano, estado_rs, sano

URL = "http://127.0.0.1:8001"
NODOS = ["mongo1", "mongo2", "mongo3"]


def consultar():
    t0 = time.time()
    try:
        with urllib.request.urlopen(URL + "/salud", timeout=15) as r:
            codigo, cuerpo = r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        codigo, cuerpo = e.code, json.loads(e.read().decode())
    return {
        "hora_utc": ahora_iso(),
        "http": codigo,
        "respuesta": cuerpo,
        "ms": round((time.time() - t0) * 1000, 1),
    }


def muestras(n=3):
    salida = []
    for _ in range(n):
        salida.append(consultar())
        time.sleep(1)
    return salida


def fase(nombre, esperado_estado, esperado_http, nodos, espera):
    acciones = []
    for n in nodos:
        docker("stop", "-t", "0", n)
        acciones.append({"hora_utc": ahora_iso(), "accion": f"docker stop -t 0 {n}"})
    if nodos:
        time.sleep(espera)
    obs = muestras()
    ok = all(
        o["respuesta"].get("estado") == esperado_estado and o["http"] == esperado_http
        for o in obs
    )
    return {
        "fase": nombre,
        "esperado": {"estado": esperado_estado, "http": esperado_http},
        "nodos_detenidos": nodos,
        "acciones": acciones,
        "observaciones": obs,
        "superado": ok,
    }


def main():
    inicio = datetime.now(timezone.utc)
    antes = estado_rs()
    if not sano(antes):
        raise SystemExit("El replica set no esta sano. Ejecuta primero proyecto2/scripts/setup.sh")
    secundarios = [m["nombre"] for m in antes if m["estado"] == "SECONDARY"]

    fases = []
    recuperacion = []
    try:
        fases.append(fase("normal", "operativo", 200, [], 0))
        fases.append(fase("degradado", "degradado", 200, [secundarios[0]], 12))
        for n in NODOS:
            docker("start", n)
        esperar_sano()
        fases.append(fase("caido", "caido", 503, list(secundarios), 15))
    finally:
        for n in NODOS:
            docker("start", n)
            recuperacion.append({"hora_utc": ahora_iso(), "accion": f"docker start {n}"})
        segundos = esperar_sano()

    despues = consultar()
    evidencia = {
        "fecha_hora_utc": inicio.isoformat(),
        "objetivo": "Comprobar que /salud del registro distingue los estados operativo, degradado y caido",
        "configuracion": {"url": URL, "falla": "docker stop -t 0"},
        "comando": "python proyecto2/pruebas/p1_salud_registro.py",
        "roles_antes": antes,
        "fases": fases,
        "recuperacion": {
            "acciones": recuperacion,
            "segundos_hasta_replica_set_sano": segundos,
            "salud_despues": despues,
        },
        "prueba_superada": all(f["superado"] for f in fases) and despues["http"] == 200,
    }
    destino = Path(__file__).resolve().parent.parent / "evidence" / "p1-salud-registro.json"
    destino.write_text(json.dumps(evidencia, indent=2), encoding="utf-8")

    for f in fases:
        r = f["observaciones"][-1]
        print(f"{f['fase']:10s} esperado {f['esperado']} observado http {r['http']} {r['respuesta']} superado {f['superado']}")
    print("Salud tras restaurar", despues["http"], despues["respuesta"])
    print("Prueba superada", evidencia["prueba_superada"])
    print("Evidencia guardada en", destino)
    raise SystemExit(0 if evidencia["prueba_superada"] else 1)


if __name__ == "__main__":
    main()