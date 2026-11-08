"""Mide la carga de anotación de texto, revisión y exportación por HTTP."""

import argparse
import json
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.client import HTTPConnection, HTTPException
from itertools import count
from math import ceil
from time import monotonic, sleep

from dataset.catalogo import SEMILLA, generar_catalogo


def simular(puerto, ventana, proyectos, usuario, anotador, revisor):
    medir_desde, terminar = ventana
    conexion = HTTPConnection("localhost", puerto, timeout=20)
    muestras = defaultdict(list)

    def pedir(metodo, ruta, datos=None, **parametros):
        inicio = monotonic()
        try:
            cuerpo = json.dumps(datos) if datos is not None else None
            conexion.request(
                metodo, ruta.format(**parametros), cuerpo,
                {"Content-Type": "application/json"},
            )
            respuesta = conexion.getresponse()
            codigo, contenido = respuesta.status, respuesta.read()
        except (OSError, HTTPException):
            conexion.close()
            codigo, contenido = "error_red", b""
        if medir_desde <= inicio < terminar:
            muestras[f"{metodo} {ruta}"].append((1000 * (monotonic() - inicio), codigo))
        return codigo, contenido

    try:
        for turno in count(usuario):
            if monotonic() >= terminar:
                break
            proyecto = proyectos[turno % len(proyectos)]
            codigo, contenido = pedir("POST", "/tareas/siguiente", {
                "proyecto_id": proyecto, "anotador_id": anotador,
            })
            if codigo != 200:
                sleep(0.5)
                continue

            asignacion = json.loads(contenido)
            tarea = asignacion["tarea"]
            ruta = {"tarea_id": tarea["id"]}
            propietario = {"anotador_id": anotador, "token": asignacion["token"]}
            enviado = False
            try:
                proximo_borrador = monotonic()
                for fin in (2, 3):
                    proximo_borrador += 2
                    sleep(max(0, proximo_borrador - monotonic()))
                    anotacion = {"spans": [{
                        "inicio": 0, "fin": fin, "etiqueta": tarea["taxonomia"][0]["nombre"],
                    }]}
                    codigo, _ = pedir("PUT", "/tareas/{tarea_id}/borrador", {
                        **propietario, "borrador": anotacion,
                    }, **ruta)
                    if codigo != 200:
                        break
                else:
                    codigo, _ = pedir("POST", "/tareas/{tarea_id}/enviar", {
                        **propietario, "anotacion": anotacion,
                    }, **ruta)
                    enviado = codigo == 200
            finally:
                if not enviado:
                    pedir("POST", "/tareas/{tarea_id}/liberar", propietario, **ruta)

            if enviado:
                for accion in ("iniciar_revision", "aprobar"):
                    codigo, _ = pedir("POST", "/tareas/{tarea_id}/revision", {
                        "accion": accion, "revisor_id": revisor,
                    }, **ruta)
                    if codigo != 200:
                        break
                else:
                    pedir("GET", "/proyectos/{proyecto_id}/exportar?limite=10", proyecto_id=proyecto)
    finally:
        conexion.close()
    return muestras


def medir(args, proyectos, anotadores, revisores):
    fecha = datetime.now(timezone.utc).isoformat()
    inicio = monotonic() + args.calentamiento
    ventana = (inicio, inicio + args.duracion)
    muestras = defaultdict(list)
    with ThreadPoolExecutor(max_workers=args.usuarios) as hilos:
        usuarios = [
            hilos.submit(
                simular, args.puerto, ventana, proyectos, i,
                anotadores[i], revisores[i % len(revisores)],
            )
            for i in range(args.usuarios)
        ]
        for usuario in usuarios:
            for ruta, intentos in usuario.result().items():
                muestras[ruta].extend(intentos)

    endpoints = {}
    for ruta, intentos in sorted(muestras.items()):
        latencias = sorted(ms for ms, _ in intentos)
        codigos = Counter(codigo for _, codigo in intentos)
        endpoints[ruta] = {
            "muestras": len(intentos),
            "codigos": {str(codigo): n for codigo, n in codigos.items()},
            "errores": sum(n for codigo, n in codigos.items()
                           if codigo == "error_red" or not 200 <= codigo < 300),
            **{
                f"p{p}_ms": round(latencias[ceil(len(latencias) * p / 100) - 1], 3)
                for p in (50, 95, 99)
            },
        }
    confirmados = sum(codigo == 200 for _, codigo in muestras["POST /tareas/{tarea_id}/enviar"])
    return {
        "inicio_utc": fecha,
        "envios_confirmados": confirmados,
        "envios_por_minuto": round(confirmados * 60 / args.duracion, 2),
        "errores": sum(e["errores"] for e in endpoints.values()),
        "endpoints": endpoints,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--usuarios", type=int, required=True)
    parser.add_argument("--calentamiento", type=int, default=30)
    parser.add_argument("--duracion", type=int, default=90)
    parser.add_argument("--corridas", type=int, default=3)
    parser.add_argument("--puerto", type=int, default=8000)
    args = parser.parse_args()

    catalogo = generar_catalogo()
    proyectos = [
        p["_id"] for p in catalogo["proyectos"]
        if p["estado"] == "activo" and p["modalidad"] == "texto"
    ]
    anotadores = [a["_id"] for a in catalogo["anotadores"] if not a["es_revisor"]]
    revisores = [a["_id"] for a in catalogo["anotadores"] if a["es_revisor"]]
    if not 1 <= args.usuarios <= len(anotadores):
        parser.error(f"Indicá entre 1 y {len(anotadores)} usuarios.")
    if args.calentamiento < 0 or min(args.duracion, args.corridas) < 1:
        parser.error("El calentamiento puede ser cero. La duración y las corridas deben ser positivas.")

    corridas = []
    for numero in range(1, args.corridas + 1):
        print(f"Corrida {numero}/{args.corridas} con {args.usuarios} usuarios", file=sys.stderr)
        corridas.append(medir(args, proyectos, anotadores, revisores))
    print(json.dumps({
        "gateway": f"http://localhost:{args.puerto}",
        "modalidad": "texto",
        "semilla": SEMILLA,
        "usuarios": args.usuarios,
        "calentamiento_seg": args.calentamiento,
        "duracion_seg": args.duracion,
        "corridas": corridas,
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()