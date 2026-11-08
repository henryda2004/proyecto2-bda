import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote

from sesion_basica import fecha, llamar


def asignar(proyecto_id, anotador_id):
    asignacion = llamar("POST", "/tareas/siguiente", {
        "proyecto_id": proyecto_id, "anotador_id": anotador_id,
    }, 200)
    propietario = {"anotador_id": anotador_id, "token": asignacion["token"]}
    return asignacion, propietario


def ruta_tarea(asignacion):
    return f"/tareas/{quote(asignacion['tarea']['id'], safe='')}"


def probar(proyecto_id, primero, segundo):
    if primero == segundo:
        raise ValueError("La prueba requiere dos anotadores distintos")

    print("P3 Vencimiento", datetime.now(timezone.utc).isoformat())
    control, propietario_control = asignar(proyecto_id, segundo)
    llamar("POST", ruta_tarea(control) + "/liberar", propietario_control, 200)

    anterior, propietario_anterior = asignar(proyecto_id, primero)
    ruta = ruta_tarea(anterior)
    vence = fecha(anterior["vence_en"])
    borrador = {"spans": [{"inicio": 0, "fin": 3, "etiqueta": "persona"}]}

    try:
        if ruta != ruta_tarea(control):
            raise RuntimeError("Los dos anotadores deben partir de la misma tarea de prueba")

        guardado = llamar("PUT", ruta + "/borrador", {
            **propietario_anterior, "borrador": borrador,
        }, 200)
        vence = fecha(guardado["vence_en"])

        recuperado = llamar("POST", "/sesiones/recuperar", propietario_anterior, 200)
        actualizado = fecha(recuperado["borrador"]["actualizado_en"])
        edad = (datetime.now(timezone.utc) - actualizado).total_seconds()
        if recuperado["tarea"]["id"] != anterior["tarea"]["id"] or recuperado["borrador"]["datos"] != borrador:
            raise RuntimeError("La tarea o el borrador recuperado difiere del original")
        print(f"Edad del borrador al recuperarlo: {edad:.3f} s")
        if not 0 <= edad <= 5:
            raise RuntimeError("La edad medida del borrador quedó fuera del rango de 0 a 5 s")

        espera = max(0, (vence - datetime.now(timezone.utc)).total_seconds()) + 1
        print(f"Candado vence {vence.isoformat()}. Esperando {espera:.1f} s")
        time.sleep(espera)
        if datetime.now(timezone.utc) < vence:
            raise RuntimeError("El plazo del candado sigue vigente")
    except BaseException:
        if datetime.now(timezone.utc) < vence:
            llamar("POST", ruta + "/liberar", propietario_anterior, 200)
        raise

    reasignada, propietario_nuevo = asignar(proyecto_id, segundo)
    ruta_nueva = ruta_tarea(reasignada)
    try:
        if ruta_nueva != ruta or propietario_nuevo["token"] == propietario_anterior["token"]:
            raise RuntimeError("Se esperaba la misma tarea con un token nuevo")
        llamar("PUT", ruta + "/borrador", {
            **propietario_anterior, "borrador": borrador,
        }, 409)
        llamar("PUT", ruta + "/borrador", {
            **propietario_nuevo, "borrador": borrador,
        }, 200)
    finally:
        llamar("POST", ruta_nueva + "/liberar", propietario_nuevo, 200)

    print("P3 OK: misma tarea reasignada, token anterior rechazado y tarea liberada")


if __name__ == "__main__":
    probar(*sys.argv[1:])
