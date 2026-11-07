"""Prueba la asignación, el guardado, la recuperación y la liberación de una tarea."""
import json
import sys
import time
from datetime import datetime
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

GATEWAY = "http://localhost:8000"


def llamar(metodo, ruta, datos, esperado):
    solicitud = Request(
        GATEWAY + ruta,
        data=json.dumps(datos).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method=metodo,
    )
    try:
        respuesta = urlopen(solicitud, timeout=25)
    except HTTPError as error:
        respuesta = error

    with respuesta:
        contenido = respuesta.read()
        cuerpo = json.loads(contenido) if contenido else {}
        evidencia = {k: v for k, v in cuerpo.items() if k != "token"}
        print(json.dumps({
            "metodo": metodo,
            "ruta": ruta,
            "http": respuesta.status,
            "x_request_id": respuesta.headers.get("X-Request-ID"),
            "respuesta": evidencia,
        }, ensure_ascii=False), flush=True)
        if respuesta.status != esperado:
            raise RuntimeError(f"Se esperaba HTTP {esperado} y llegó {respuesta.status}")
        return cuerpo


def fecha(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def probar(proyecto_id, anotador_id):
    asignacion = llamar("POST", "/tareas/siguiente", {
        "proyecto_id": proyecto_id,
        "anotador_id": anotador_id,
    }, 200)
    tarea_id = asignacion["tarea"]["id"]
    ruta = f"/tareas/{quote(tarea_id, safe='')}"
    propietario = {"anotador_id": anotador_id, "token": asignacion["token"]}
    borrador = {"spans": [{"inicio": 0, "fin": 3, "etiqueta": "persona"}]}

    try:
        time.sleep(2)
        guardado = llamar("PUT", ruta + "/borrador", {
            **propietario, "borrador": borrador,
        }, 200)
        assert fecha(guardado["vence_en"]) > fecha(asignacion["vence_en"])

        llamar("PUT", ruta + "/borrador", {
            **propietario, "token": "token-incorrecto", "borrador": {},
        }, 409)
        recuperado = llamar("POST", "/sesiones/recuperar", propietario, 200)
        assert recuperado["tarea"]["id"] == tarea_id
        assert recuperado["borrador"]["datos"] == borrador
    finally:
        llamar("POST", ruta + "/liberar", propietario, 200)

    print("Verificado: plazo renovado, token incorrecto rechazado y borrador recuperado.")


if __name__ == "__main__":
    probar(sys.argv[1], sys.argv[2])
