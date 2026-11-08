import subprocess
import sys
import time
from datetime import datetime, timezone
from multiprocessing import Event, Process
from urllib.parse import quote

from sesion_basica import llamar

COMPOSE = [
    "docker", "compose", "-f", "docker-compose.yml",
    "-f", "docker-compose.dev.yml", "-f", "docker-compose.gateway.yml",
]


def valkey(*args):
    return subprocess.run(
        [*COMPOSE, "exec", "-T", "valkey", "valkey-cli", "--raw", *args],
        check=True, capture_output=True, text=True, timeout=4,
    ).stdout.strip()


def editar(ruta, propietario, borrador, guardado):
    llamar("PUT", ruta + "/borrador", {
        **propietario, "borrador": borrador,
    }, 200)
    guardado.set()
    time.sleep(60)


def verificar_borrador(propietario, tarea_id, borrador):
    recuperado = llamar("POST", "/sesiones/recuperar", propietario, 200)
    if recuperado["tarea"]["id"] != tarea_id or recuperado["borrador"]["datos"] != borrador:
        raise RuntimeError("La tarea o el borrador recuperado difiere del original")


def probar(proyecto_id, anotador_id):
    print("P8 Valkey", datetime.now(timezone.utc).isoformat())
    campos = valkey("CONFIG", "GET", "appendonly", "appendfsync").splitlines()
    config = dict(zip(campos[::2], campos[1::2]))
    print("Persistencia", config)
    if config.get("appendonly") != "yes":
        raise RuntimeError("La prueba requiere Valkey con AOF activo")

    asignacion = llamar("POST", "/tareas/siguiente", {
        "proyecto_id": proyecto_id, "anotador_id": anotador_id,
    }, 200)
    tarea_id = asignacion["tarea"]["id"]
    ruta = f"/tareas/{quote(tarea_id, safe='')}"
    propietario = {"anotador_id": anotador_id, "token": asignacion["token"]}
    borrador = {"spans": [{"inicio": 0, "fin": 3, "etiqueta": "persona"}]}

    try:
        guardado = Event()
        cliente = Process(target=editar, args=(ruta, propietario, borrador, guardado))
        cliente.start()
        try:
            if not guardado.wait(timeout=27):
                raise RuntimeError("El cliente no confirmó el guardado del borrador")
        finally:
            cliente.terminate()
            cliente.join()

        print(f"Cliente interrumpido con código {cliente.exitcode}")
        verificar_borrador(propietario, tarea_id, borrador)
        print("OK: borrador recuperado después de interrumpir el cliente")

        time.sleep(2)
        reinicio = [*COMPOSE, "restart", "--no-deps", "valkey"]
        print("Comando de reinicio:", " ".join(reinicio))
        inicio = time.monotonic()
        try:
            subprocess.run(reinicio, check=True, timeout=30)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            subprocess.run([*COMPOSE, "start", "valkey"], check=True, timeout=20)
            raise

        for _ in range(10):
            try:
                if valkey("PING") == "PONG":
                    break
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                pass
            time.sleep(0.5)
        else:
            raise RuntimeError("Valkey no respondió PONG después del reinicio")

        verificar_borrador(propietario, tarea_id, borrador)
        print(f"OK: mismo borrador tras reiniciar Valkey en {time.monotonic() - inicio:.3f} s")
    finally:
        llamar("POST", ruta + "/liberar", propietario, 200)

    print("P8 Valkey OK: sesión recuperada, borrador conservado y tarea liberada")


if __name__ == "__main__":
    probar(sys.argv[1], sys.argv[2])
