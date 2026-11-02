import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pymongo.errors import PyMongoError
from pymongo.write_concern import WriteConcern
from pymongo import ReturnDocument

from db import client, db

app = FastAPI(title="Servicio de registro")


class Etiqueta(BaseModel):
    nombre: str
    instruccion: str


class ProyectoIn(BaseModel):
    cliente_id: str
    nombre: str
    modalidad: Literal["texto", "imagen", "video", "lidar"]
    taxonomia: list[Etiqueta] = Field(min_length=1)
    reglas: str = ""
    sla_horas: int = Field(default=48, gt=0)

GEOMETRIAS = {
    "texto": {"span"},
    "imagen": {"bbox", "poligono"},
    "video": {"bbox_video"},
    "lidar": {"cuboide_3d"},
}


class TareaIn(BaseModel):
    uri: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    bytes: int = Field(gt=0)
    mime: str
    meta: dict = {}
    tipo_tarea: Literal["span", "bbox", "poligono", "bbox_video", "cuboide_3d"]
    prioridad: int = Field(default=0, ge=0)


class LoteIn(BaseModel):
    nombre: str
    tareas: list[TareaIn] = Field(min_length=1, max_length=5000)


class SiguienteIn(BaseModel):
    anotador_id: str
    proyecto_id: str
    plazo_seg: int = Field(default=120, gt=0)

@app.exception_handler(PyMongoError)
async def mongo_no_responde(request, exc):
    return JSONResponse(
        status_code=503,
        content={"detalle": "El registro no esta disponible"},
    )


@app.get("/salud")
def salud():
    try:
        client.admin.command("ping")
        return {"servicio": "registro", "estado": "operativo"}
    except Exception:
        return JSONResponse(
            status_code=503,
            content={"servicio": "registro", "estado": "caido"},
        )


@app.post("/proyectos", status_code=201)
def crear_proyecto(datos: ProyectoIn):
    if db.clientes.find_one({"_id": datos.cliente_id}, {"_id": 1}) is None:
        raise HTTPException(status_code=404, detail="El cliente no existe")

    proyecto = {
        "_id": str(uuid.uuid4()),
        "cliente_id": datos.cliente_id,
        "nombre": datos.nombre,
        "modalidad": datos.modalidad,
        "taxonomia": [e.model_dump() for e in datos.taxonomia],
        "reglas": datos.reglas,
        "sla_horas": datos.sla_horas,
        "estado": "activo",
        "creado_en": datetime.now(timezone.utc),
    }
    db.proyectos.insert_one(proyecto)
    return {"id": proyecto["_id"], "estado": proyecto["estado"]}

@app.post("/proyectos/{proyecto_id}/lotes", status_code=201)
def registrar_lote(proyecto_id: str, datos: LoteIn):
    proyecto = db.proyectos.find_one({"_id": proyecto_id})
    if proyecto is None:
        raise HTTPException(status_code=404, detail="El proyecto no existe")
    if proyecto["estado"] != "activo":
        raise HTTPException(status_code=409, detail="El proyecto no esta activo")

    modalidad = proyecto["modalidad"]
    invalidos = {t.tipo_tarea for t in datos.tareas} - GEOMETRIAS[modalidad]
    if invalidos:
        raise HTTPException(
            status_code=422,
            detail=f"tipo_tarea no valido para {modalidad}: {sorted(invalidos)}",
        )

    batch_id = str(uuid.uuid4())
    ahora = datetime.now(timezone.utc)

    lote = {
        "_id": batch_id,
        "proyecto_id": proyecto_id,
        "nombre": datos.nombre,
        "total_tareas": len(datos.tareas),
        "creado_en": ahora,
    }
    tareas = [
        {
            "_id": str(uuid.uuid4()),
            "proyecto_id": proyecto_id,
            "batch_id": batch_id,
            "cliente_id": proyecto["cliente_id"],
            "modalidad": modalidad,
            "tipo_tarea": t.tipo_tarea,
            "uri": t.uri,
            "sha256": t.sha256,
            "bytes": t.bytes,
            "mime": t.mime,
            "meta": t.meta,
            "prioridad": t.prioridad,
            "estado": "disponible",
            "version": 0,
            "asignacion": None,
            "anotacion": None,
            "ultima_revision": None,
            "ultimo_evento_id": None,
            "creado_en": ahora,
        }
        for t in datos.tareas
    ]

    def escribir(session):
        db.batches.insert_one(lote, session=session)
        db.tareas.insert_many(tareas, session=session)

    with client.start_session() as session:
        session.with_transaction(escribir, write_concern=WriteConcern("majority"))

    return {"id": batch_id, "tareas": len(tareas)}

@app.post("/tareas/siguiente")
def siguiente_tarea(datos: SiguienteIn):
    anotador = db.anotadores.find_one({"_id": datos.anotador_id})
    if anotador is None:
        raise HTTPException(status_code=404, detail="El anotador no existe")
    proyecto = db.proyectos.find_one({"_id": datos.proyecto_id})
    if proyecto is None:
        raise HTTPException(status_code=404, detail="El proyecto no existe")

    ahora = datetime.now(timezone.utc)
    token = str(uuid.uuid4())
    evento_id = str(uuid.uuid4())

    tarea = db.tareas.find_one_and_update(
        {
            "proyecto_id": datos.proyecto_id,
            "$or": [
                {"estado": "disponible"},
                {"estado": "abierta", "asignacion.vence_en": {"$lt": ahora}},
            ],
        },
        {
            "$set": {
                "estado": "abierta",
                "asignacion": {
                    "anotador_id": datos.anotador_id,
                    "token": token,
                    "tomada_en": ahora,
                    "vence_en": ahora + timedelta(seconds=datos.plazo_seg),
                },
                "ultimo_evento_id": evento_id,
            },
            "$inc": {"version": 1},
        },
        sort=[("prioridad", -1)],
        return_document=ReturnDocument.AFTER,
    )
    if tarea is None:
        return Response(status_code=204)

    evento = {
        "event_id": evento_id,
        "event_ts": ahora,
        "tarea_id": tarea["_id"],
        "version": tarea["version"],
        "estado": "abierta",
        "cliente_id": tarea["cliente_id"],
        "proyecto_id": tarea["proyecto_id"],
        "batch_id": tarea["batch_id"],
        "anotador_id": datos.anotador_id,
        "equipo_id": anotador["equipo_id"],
        "revisor_id": None,
        "modalidad": tarea["modalidad"],
        "tipo_tarea": tarea["tipo_tarea"],
        "duracion_seg": None,
        "motivo_rechazo": None,
        "sla_horas": proyecto["sla_horas"],
    }

    return {
        "tarea": {
            "id": tarea["_id"],
            "proyecto_id": tarea["proyecto_id"],
            "uri": tarea["uri"],
            "sha256": tarea["sha256"],
            "bytes": tarea["bytes"],
            "mime": tarea["mime"],
            "meta": tarea["meta"],
            "modalidad": tarea["modalidad"],
            "tipo_tarea": tarea["tipo_tarea"],
            "taxonomia": proyecto["taxonomia"],
            "reglas": proyecto["reglas"],
        },
        "token": token,
        "vence_en": tarea["asignacion"]["vence_en"],
        "evento": evento,
    }