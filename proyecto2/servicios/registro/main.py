import uuid
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pymongo.errors import PyMongoError
from pymongo.write_concern import WriteConcern

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