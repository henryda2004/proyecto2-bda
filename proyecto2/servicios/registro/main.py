import uuid
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pymongo.errors import PyMongoError

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