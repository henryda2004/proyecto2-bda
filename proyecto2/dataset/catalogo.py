"""Catalogo base compartido (clientes, proyectos, anotadores y equipos).

Es determinista. Con la misma semilla genera siempre los mismos identificadores,
para que el registro (MongoDB) y el historial (Parquet) hablen de las mismas
entidades. Solo usa la biblioteca estandar.
"""
import random
import uuid
from datetime import datetime, timedelta, timezone

SEMILLA = 2026
N_CLIENTES = 60
N_PROYECTOS_REGISTRO = 120
N_PROYECTOS_CERRADOS = 60
N_EQUIPOS = 30
ANOTADORES_POR_EQUIPO = 10
INICIO = datetime(2025, 11, 1, tzinfo=timezone.utc)
MODALIDADES = ["texto", "imagen", "video", "lidar"]

TAXONOMIAS = {
    "texto": [
        {"nombre": "texto#persona", "instruccion": "Nombre de una persona"},
        {"nombre": "texto#organizacion", "instruccion": "Empresa o institucion"},
        {"nombre": "texto#lugar", "instruccion": "Ciudad, pais o direccion"},
        {"nombre": "texto#fecha", "instruccion": "Fecha o periodo"},
    ],
    "imagen": [
        {"nombre": "imagen#objeto", "instruccion": "Objeto principal de la escena"},
        {"nombre": "imagen#defecto", "instruccion": "Defecto o dano visible"},
        {"nombre": "imagen#texto", "instruccion": "Texto legible dentro de la imagen"},
    ],
    "video": [
        {"nombre": "video#persona", "instruccion": "Persona en movimiento"},
        {"nombre": "video#vehiculo", "instruccion": "Vehiculo en movimiento"},
    ],
    "lidar": [
        {"nombre": "lidar#peaton", "instruccion": "Persona a pie"},
        {"nombre": "lidar#vehiculo", "instruccion": "Vehiculo motorizado"},
        {"nombre": "lidar#ciclista", "instruccion": "Ciclista. Un skater se clasifica como ciclista y no como peaton"},
        {"nombre": "lidar#signal", "instruccion": "Senal de transito"},
    ],
}


def uid(rng):
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def generar_catalogo(semilla=SEMILLA):
    rng = random.Random(semilla)

    clientes = [
        {"_id": uid(rng), "nombre": f"Cliente {i:02d}"}
        for i in range(1, N_CLIENTES + 1)
    ]

    modalidades = ["texto"] * 36 + ["imagen"] * 36 + ["video"] * 24 + ["lidar"] * 24
    rng.shuffle(modalidades)

    proyectos = []
    for i in range(N_PROYECTOS_REGISTRO + N_PROYECTOS_CERRADOS):
        activo = i < N_PROYECTOS_REGISTRO
        modalidad = modalidades[i] if activo else rng.choice(MODALIDADES)
        proyectos.append(
            {
                "_id": uid(rng),
                "cliente_id": clientes[i % N_CLIENTES]["_id"],
                "nombre": f"Proyecto {i + 1:03d} {modalidad}",
                "modalidad": modalidad,
                "taxonomia": TAXONOMIAS[modalidad],
                "reglas": "Seguir la guia de anotacion del cliente.",
                "sla_horas": 48,
                "estado": "activo" if activo else "cerrado",
                "creado_en": INICIO + timedelta(days=rng.randint(0, 330)),
            }
        )

    anotadores = []
    for e in range(1, N_EQUIPOS + 1):
        for k in range(ANOTADORES_POR_EQUIPO):
            anotadores.append(
                {
                    "_id": uid(rng),
                    "nombre": f"Anotador {len(anotadores) + 1:03d}",
                    "equipo_id": f"equipo-{e:02d}",
                    "es_revisor": k == 0,
                }
            )

    return {"clientes": clientes, "proyectos": proyectos, "anotadores": anotadores}