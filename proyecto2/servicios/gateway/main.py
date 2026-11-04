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
from contextlib import asynccontextmanager

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


async def reenviar(request: Request, metodo: str, ruta: str, datos=None):
    try:
        respuesta = await request.app.state.http.request(
            metodo,
            f"{SERVICIOS['registro']}{ruta}",
            json=datos,
            params=request.query_params.multi_items(),
            headers={"X-Request-ID": request.state.request_id},
        )
    except httpx.RequestError:
        raise HTTPException(
            status_code=503,
            detail="El registro no esta disponible",
        )

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