"""
Implementa la primera parte del gateway de la plataforma.
Consulta la salud de los servicios y reenvia al registro las
solicitudes para crear proyectos, registrar lotes y exportar
tareas aprobadas, conservando sus respuestas HTTP.
Tambien agrega un identificador a cada solicitud para facilitar
su seguimiento y devuelve 503 si no puede contactar al registro.
"""



import asyncio
import os
import uuid
import json
import logging 
from contextlib import asynccontextmanager
from typing import Literal
from urllib.parse import quote
from pydantic import BaseModel, Field

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

SERVICIOS = {
    nombre: os.getenv(
        f"{nombre.upper()}_URL", f"http://{nombre}:8000"
    ).rstrip("/")
    for nombre in ("registro", "sesion", "historial", "analitica")
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient(timeout=10.0) as cliente:
        app.state.http = cliente
        yield


app = FastAPI(
    title="Gateway de la plataforma",
    lifespan=lifespan,
)


@app.middleware("http")
async def identificar_solicitud(request: Request, call_next):
    request.state.request_id = (
        request.headers.get("X-Request-ID") or str(uuid.uuid4())
    )
    respuesta = await call_next(request)
    respuesta.headers["X-Request-ID"] = request.state.request_id
    return respuesta


async def reenviar(request: Request, metodo: str, ruta: str, datos=None, *, servicio="registro"):
    try:
        respuesta = await request.app.state.http.request(
            metodo,
            f"{SERVICIOS[servicio]}{ruta}",
            json=datos,
            params=request.query_params.multi_items(),
            headers={"X-Request-ID": request.state.request_id},
        )
    except httpx.RequestError:
        detalle = (
            "El registro no esta disponible"
            if servicio == "registro" else f"El servicio {servicio} no esta disponible"
        )
        return JSONResponse(status_code=503, content={"detail": detalle})

    headers = {}
    if "content-type" in respuesta.headers:
        headers["content-type"] = respuesta.headers["content-type"]

    return Response(
        content=respuesta.content,
        status_code=respuesta.status_code,
        headers=headers,
    )


async def consultar_salud(cliente, url):
    try:
        respuesta = await cliente.get(f"{url}/salud", timeout=2.0)
        datos = respuesta.json()

        if not isinstance(datos, dict):
            return {"estado": "caido"}

        estado = datos.get("estado")
        if (
            not respuesta.is_success
            or estado not in {"operativo", "degradado", "caido"}
        ):
            estado = "caido"

        return {**datos, "estado": estado}

    except (httpx.RequestError, ValueError):
        return {"estado": "caido"}


@app.get("/salud")
async def salud(request: Request):
    resultados = await asyncio.gather(
        *(
            consultar_salud(request.app.state.http, url)
            for url in SERVICIOS.values()
        )
    )

    capas = dict(zip(SERVICIOS, resultados))
    estados = [capa["estado"] for capa in resultados]

    if all(estado == "operativo" for estado in estados):
        estado = "operativo"
    elif all(estado == "caido" for estado in estados):
        estado = "caido"
    else:
        estado = "degradado"

    return JSONResponse(
        status_code=503 if estado == "caido" else 200,
        content={
            "servicio": "gateway",
            "estado": estado,
            "capas": capas,
        },
    )


@app.post("/proyectos")
async def crear_proyecto(request: Request, datos: dict):
    return await reenviar(request, "POST", "/proyectos", datos)


@app.post("/proyectos/{proyecto_id}/lotes")
async def registrar_lote(
    proyecto_id: str, request: Request, datos: dict
):
    return await reenviar(
        request,
        "POST",
        f"/proyectos/{proyecto_id}/lotes",
        datos,
    )


@app.get("/proyectos/{proyecto_id}/exportar")
async def exportar(proyecto_id: str, request: Request):
    return await reenviar(
        request,
        "GET",
        f"/proyectos/{proyecto_id}/exportar",
    )
async def cerrar_lock(request, tarea_id, propietario):
    respuesta = await reenviar(
        request, "POST", f"/tareas/{quote(tarea_id, safe='')}/liberar",
        propietario, servicio="sesion",
    )
    if respuesta.status_code != 204:
        logging.warning("El candado de la tarea %s queda pendiente de vencer", tarea_id)


@app.post("/tareas/siguiente")
async def siguiente_tarea(request: Request, datos: dict):
    asignacion = await reenviar(
        request, "POST", "/tareas/siguiente", {**datos, "plazo_seg": 120}
    )
    if asignacion.status_code != 200:
        return asignacion

    cuerpo = json.loads(asignacion.body)
    tarea = cuerpo["tarea"]
    propietario = {"anotador_id": datos["anotador_id"], "token": cuerpo["token"]}
    sesion = await reenviar(
        request, "POST", "/sesiones",
        {**propietario, "tarea": tarea, "vence_en": cuerpo["vence_en"]},
        servicio="sesion",
    )
    if sesion.status_code == 201:
        cuerpo["borrador"] = json.loads(sesion.body)["borrador"]
        return cuerpo

    await cerrar_lock(request, tarea["id"], propietario)
    ruta = f"/tareas/{quote(tarea['id'], safe='')}/transicion"
    reversa = await reenviar(request, "POST", ruta, {**propietario, "accion": "liberar"})
    if reversa.status_code not in (200, 404, 409):
        logging.warning("La asignación de la tarea %s queda pendiente de vencer", tarea["id"])
    return sesion


@app.post("/tareas/{tarea_id}/liberar")
async def liberar_tarea(tarea_id: str, request: Request, datos: dict):
    ruta = f"/tareas/{quote(tarea_id, safe='')}/transicion"
    respuesta = await reenviar(request, "POST", ruta, {**datos, "accion": "liberar"})
    if respuesta.status_code == 200:
        await cerrar_lock(request, tarea_id, datos)
    return respuesta

class PropietarioIn(BaseModel):
    anotador_id: str = Field(min_length=1)
    token: str = Field(min_length=1)


class BorradorIn(PropietarioIn):
    borrador: dict


class EnvioIn(PropietarioIn):
    anotacion: dict


async def validar_candado(request: Request, tarea_id: str, propietario: dict):
    sesion = await reenviar(
        request, "POST", "/sesiones/recuperar", propietario, servicio="sesion"
    )
    if sesion.status_code == 404:
        return JSONResponse(status_code=409, content={"detail": "La sesión no está vigente"})
    if sesion.status_code != 200:
        return sesion
    if json.loads(sesion.body)["tarea"]["id"] != tarea_id:
        return JSONResponse(status_code=409, content={"detail": "El candado es de otra tarea"})
    return None


@app.put("/tareas/{tarea_id}/borrador")
async def guardar_borrador(tarea_id: str, request: Request, datos: BorradorIn):
    propietario = datos.model_dump(exclude={"borrador"})
    error = await validar_candado(request, tarea_id, propietario)
    if error is not None:
        return error

    tarea = quote(tarea_id, safe="")
    plazo = await reenviar(
        request, "POST", f"/tareas/{tarea}/transicion",
        {**propietario, "accion": "renovar"},
    )
    if plazo.status_code != 200:
        return plazo

    cuerpo = {**datos.model_dump(), "vence_en": json.loads(plazo.body)["vence_en"]}
    return await reenviar(
        request, "PUT", f"/tareas/{tarea}/borrador", cuerpo, servicio="sesion"
    )


@app.post("/tareas/{tarea_id}/enviar")
async def enviar_tarea(tarea_id: str, request: Request, datos: EnvioIn):
    propietario = datos.model_dump(exclude={"anotacion"})
    error = await validar_candado(request, tarea_id, propietario)
    if error is not None:
        return error

    tarea = quote(tarea_id, safe="")
    respuesta = await reenviar(
        request, "POST", f"/tareas/{tarea}/transicion",
        {**datos.model_dump(), "accion": "enviar"},
    )
    if respuesta.status_code == 200:
        await cerrar_lock(request, tarea_id, propietario)
    return respuesta


@app.post("/sesiones/recuperar")
async def recuperar_sesion(request: Request, datos: dict):
    return await reenviar(
        request, "POST", "/sesiones/recuperar", datos, servicio="sesion"
    )
    


class RevisionIn(BaseModel):
    accion: Literal["iniciar_revision", "aprobar", "rechazar"]
    revisor_id: str = Field(min_length=1)
    motivo: str | None = None


@app.post("/tareas/{tarea_id}/revision")
async def revisar_tarea(tarea_id: str, request: Request, datos: RevisionIn):
    tarea = quote(tarea_id, safe="")
    return await reenviar(
        request,
        "POST",
        f"/tareas/{tarea}/transicion",
        datos.model_dump(exclude_none=True),
    )