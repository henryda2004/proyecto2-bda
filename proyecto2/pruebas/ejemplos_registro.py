"""Recorre las seis rutas del registro y guarda cada llamada con su respuesta real.

Sirve de base para documentar proyecto2/endpoints/registro.md con ejemplos verificados.
Salida: proyecto2/evidence/registro-ejemplos.json
"""
import hashlib
import json
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

URL = "http://127.0.0.1:8001"
LLAMADAS = []


def llamar(nombre, metodo, ruta, cuerpo=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(
        URL + ruta, data=datos, method=metodo, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            texto = r.read().decode()
            estado = r.status
    except urllib.error.HTTPError as e:
        texto = e.read().decode()
        estado = e.code
    respuesta = json.loads(texto) if texto else None
    LLAMADAS.append(
        {
            "nombre": nombre,
            "peticion": {"metodo": metodo, "ruta": ruta, "cuerpo": cuerpo},
            "http": estado,
            "respuesta": respuesta,
        }
    )
    print(f"{estado}  {metodo:4s} {ruta}  ({nombre})")
    return estado, respuesta


def preparar_datos():
    js = (
        "const d = db.getSiblingDB('plataforma');"
        "d.clientes.updateOne({_id:'cli-ejemplo'},{$set:{nombre:'Cliente ejemplo'}},{upsert:true});"
        "d.anotadores.updateOne({_id:'ana-ejemplo'},{$set:{nombre:'ana-ejemplo',equipo_id:'equipo-01',es_revisor:false}},{upsert:true});"
        "print('ok')"
    )
    subprocess.run(
        ["docker", "exec", "mongo1", "mongosh", "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0", "--quiet", "--eval", js],
        capture_output=True,
        text=True,
        check=True,
    )


def tarea(i):
    return {
        "uri": f"s3://ejemplo/escena-{i}.pcd",
        "sha256": hashlib.sha256(f"ejemplo-{i}".encode()).hexdigest(),
        "bytes": 1048576,
        "mime": "application/octet-stream",
        "meta": {},
        "tipo_tarea": "cuboide_3d",
        "prioridad": 0,
    }


def tomar(proyecto_id, nombre):
    estado, cuerpo = llamar(
        nombre,
        "POST",
        "/tareas/siguiente",
        {"anotador_id": "ana-ejemplo", "proyecto_id": proyecto_id, "plazo_seg": 600},
    )
    return cuerpo


def main():
    preparar_datos()
    llamar("salud", "GET", "/salud")

    llamar("proyecto_invalido", "POST", "/proyectos", {"cliente_id": "cli-ejemplo"})
    llamar(
        "proyecto_cliente_inexistente",
        "POST",
        "/proyectos",
        {
            "cliente_id": "no-existe",
            "nombre": "x",
            "modalidad": "lidar",
            "taxonomia": [{"nombre": "a", "instruccion": "b"}],
        },
    )
    _, proyecto = llamar(
        "crear_proyecto",
        "POST",
        "/proyectos",
        {
            "cliente_id": "cli-ejemplo",
            "nombre": "Proyecto de ejemplo",
            "modalidad": "lidar",
            "taxonomia": [{"nombre": "lidar#peaton", "instruccion": "Persona a pie"}],
            "reglas": "Marcar todos los peatones visibles",
            "sla_horas": 48,
        },
    )
    pid = proyecto["id"]

    llamar("crear_lote", "POST", f"/proyectos/{pid}/lotes", {"nombre": "lote-ejemplo", "tareas": [tarea(i) for i in range(3)]})
    llamar("lote_proyecto_inexistente", "POST", "/proyectos/no-existe/lotes", {"nombre": "x", "tareas": [tarea(99)]})

    t1 = tomar(pid, "siguiente_tarea")
    tid, token = t1["tarea"]["id"], t1.get("token")
    llamar("enviar_token_incorrecto", "POST", f"/tareas/{tid}/transicion", {"accion": "enviar", "anotador_id": "ana-ejemplo", "token": "incorrecto", "anotacion": {}})
    llamar("enviar", "POST", f"/tareas/{tid}/transicion", {"accion": "enviar", "anotador_id": "ana-ejemplo", "token": token, "anotacion": {"cuboides": [{"x": 1, "y": 2, "z": 3}]}})
    llamar("enviar_otra_vez", "POST", f"/tareas/{tid}/transicion", {"accion": "enviar", "anotador_id": "ana-ejemplo", "token": token, "anotacion": {}})
    llamar("iniciar_revision", "POST", f"/tareas/{tid}/transicion", {"accion": "iniciar_revision", "revisor_id": "rev-ejemplo"})
    llamar("aprobar", "POST", f"/tareas/{tid}/transicion", {"accion": "aprobar", "revisor_id": "rev-ejemplo"})

    t2 = tomar(pid, "siguiente_tarea_2")
    tid2, token2 = t2["tarea"]["id"], t2.get("token")
    llamar("enviar_2", "POST", f"/tareas/{tid2}/transicion", {"accion": "enviar", "anotador_id": "ana-ejemplo", "token": token2, "anotacion": {}})
    llamar("iniciar_revision_2", "POST", f"/tareas/{tid2}/transicion", {"accion": "iniciar_revision", "revisor_id": "rev-ejemplo"})
    llamar("rechazar", "POST", f"/tareas/{tid2}/transicion", {"accion": "rechazar", "revisor_id": "rev-ejemplo", "motivo": "Falta un peaton"})

    t3 = tomar(pid, "siguiente_tarea_3")
    tid3, token3 = t3["tarea"]["id"], t3.get("token")
    llamar("liberar", "POST", f"/tareas/{tid3}/transicion", {"accion": "liberar", "anotador_id": "ana-ejemplo", "token": token3})

    llamar("siguiente_proyecto_inexistente", "POST", "/tareas/siguiente", {"anotador_id": "ana-ejemplo", "proyecto_id": "no-existe"})
    _, pagina = llamar("exportar_pagina_1", "GET", f"/proyectos/{pid}/exportar?limite=2")
    if isinstance(pagina, dict):
        for clave, valor in pagina.items():
            if isinstance(valor, str) and clave != "proyecto_id":
                llamar("exportar_pagina_2", "GET", f"/proyectos/{pid}/exportar?limite=2&{clave}={valor}")
                break
    llamar("exportar_proyecto_inexistente", "GET", "/proyectos/no-existe/exportar")

    destino = Path(__file__).resolve().parent.parent / "evidence" / "registro-ejemplos.json"
    destino.write_text(json.dumps(LLAMADAS, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Guardado en", destino)


if __name__ == "__main__":
    main()