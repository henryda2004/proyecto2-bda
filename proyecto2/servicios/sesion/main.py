"""Conecta el servicio de sesion con Valkey y comprueba su AOF.
Publica un mensaje de salud cada cinco segundos con vencimiento
de quince segundos, y expone GET /salud para el gateway.
"""
import asyncio
import json
import os
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone

import valkey.asyncio as valkey
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from valkey.exceptions import ValkeyError
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

async def consultar_estado(cliente):
    try:
        persistencia = await cliente.info("persistence")
        configuracion = await cliente.config_get("appendfsync")

        aof = persistencia.get("aof_enabled") == 1
        fsync = configuracion.get("appendfsync")
        escritura = persistencia.get("aof_last_write_status")

        correcto = aof and fsync == "everysec" and escritura == "ok"

        cuerpo = {
            "servicio": "sesion",
            "estado": "operativo" if correcto else "degradado",
            "aof": aof,
            "appendfsync": fsync,
            "actualizado_en": datetime.now(timezone.utc).isoformat(),
        }

        await cliente.set("salud:sesion", json.dumps(cuerpo), ex=15)
        return cuerpo

    except ValkeyError:
        return {
            "servicio": "sesion",
            "estado": "caido",
            "detalle": "Valkey no responde o no permite escribir",
        }


async def publicar_salud(cliente):
    while True:
        await consultar_estado(cliente)
        await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cliente = valkey.from_url(
        os.getenv("VALKEY_URL", "valkey://valkey:6379/0"),
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=1,
        retry_on_timeout=False,
    )

    app.state.valkey = cliente
    publicador = asyncio.create_task(publicar_salud(cliente))

    try:
        yield
    finally:
        publicador.cancel()
        with suppress(asyncio.CancelledError):
            await publicador
        await cliente.aclose()


app = FastAPI(title="Servicio de sesion", lifespan=lifespan)


@app.get("/salud")
async def salud(request: Request):
    cuerpo = await consultar_estado(request.app.state.valkey)

    return JSONResponse(
        status_code=503 if cuerpo["estado"] == "caido" else 200,
        content=cuerpo,
    )
    
class Propietario(BaseModel):
    anotador_id: str = Field(min_length=1)
    token: str = Field(min_length=1)


class TareaSesion(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str
    proyecto_id: str


class AbrirSesion(Propietario):
    tarea: TareaSesion
    vence_en: AwareDatetime


# Crea juntos el candado, la sesión y la entrada de edición.
ABRIR = """
local token, anotador, tarea, proyecto = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local tiempo = redis.call('TIME')
local ahora = tiempo[1] * 1000 + math.floor(tiempo[2] / 1000)
local vence = math.min(tonumber(ARGV[5]), ahora + 120000)
if vence <= ahora or redis.call('EXISTS', KEYS[2], KEYS[4]) > 0 then
    return 0
end
if not redis.call('SET', KEYS[1], token, 'NX', 'PX', vence - ahora) then
    return 0
end
redis.call('HSET', KEYS[2], 'token', token, 'tarea_id', tarea,
    'proyecto_id', proyecto, 'etapa', 'editando', 'tarea', ARGV[6])
redis.call('PEXPIREAT', KEYS[2], vence)
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', ahora)
redis.call('ZADD', KEYS[3], vence, anotador)
redis.call('PEXPIRE', KEYS[3], 120000)
if redis.call('HGET', KEYS[5], 'anotador_id') == anotador then
    return {redis.call('HGET', KEYS[5], 'contenido') or ''}
end
return {''}
"""


# El token protege el candado y evita reabrir una sesión ya cerrada.
CERRAR = """
redis.call('SET', KEYS[3], '1', 'PX', 120000)
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
end
if redis.call('HGET', KEYS[2], 'token') == ARGV[1]
    and redis.call('HGET', KEYS[2], 'tarea_id') == ARGV[3] then
    local proyecto = redis.call('HGET', KEYS[2], 'proyecto_id')
    if proyecto then
        redis.call('ZREM', 'editando:' .. proyecto, ARGV[2])
    end
    redis.call('DEL', KEYS[2])
end
return 1
"""


@app.exception_handler(ValkeyError)
async def valkey_no_disponible(request: Request, exc: ValkeyError):
    return JSONResponse(status_code=503, content={"detail": "Valkey no está disponible"})

@app.post("/sesiones", status_code=201)
async def abrir_sesion(request: Request, datos: AbrirSesion):
    tarea = datos.tarea
    creado = await request.app.state.valkey.eval(
        ABRIR, 5,
        f"lock:tarea:{tarea.id}", f"sesion:{datos.anotador_id}",
        f"editando:{tarea.proyecto_id}", f"cierre:sesion:{datos.token}",
        f"borrador:{tarea.id}",
        datos.token, datos.anotador_id, tarea.id, tarea.proyecto_id,
        int(datos.vence_en.timestamp() * 1000), tarea.model_dump_json(),
    )
    if not creado:
        raise HTTPException(409, "La sesión está ocupada o venció")
    return {
        "tarea_id": tarea.id,
        "borrador": json.loads(creado[0]) if creado[0] else None,
    }


@app.post("/tareas/{tarea_id}/liberar", status_code=204)
async def cerrar_sesion(tarea_id: str, request: Request, datos: Propietario):
    await request.app.state.valkey.eval(
        CERRAR, 3,
        f"lock:tarea:{tarea_id}", f"sesion:{datos.anotador_id}",
        f"cierre:sesion:{datos.token}",
        datos.token, datos.anotador_id, tarea_id,
    )
    return Response(status_code=204)

class BorradorIn(Propietario):
    borrador: dict


# Comprueba el candado y guarda o lee el borrador en una sola operación.
BORRADOR = """
local token, anotador, tarea, contenido = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
if redis.call('GET', KEYS[1]) ~= token
    or redis.call('HGET', KEYS[2], 'token') ~= token
    or redis.call('HGET', KEYS[2], 'tarea_id') ~= tarea then
    return nil
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl <= 0 then
    return nil
end
if contenido ~= '' then
    redis.call('HSET', KEYS[3], 'anotador_id', anotador, 'contenido', contenido)
    redis.call('PERSIST', KEYS[3])
end
local borrador = ''
if redis.call('HGET', KEYS[3], 'anotador_id') == anotador then
    borrador = redis.call('HGET', KEYS[3], 'contenido') or ''
end
local tiempo = redis.call('TIME')
local vence = tiempo[1] * 1000 + math.floor(tiempo[2] / 1000) + ttl
return {redis.call('HGET', KEYS[2], 'tarea'),
    redis.call('HGET', KEYS[2], 'etapa'), borrador, vence}
"""


async def operar_borrador(request, tarea_id, datos, contenido=""):
    resultado = await request.app.state.valkey.eval(
        BORRADOR, 3,
        f"lock:tarea:{tarea_id}", f"sesion:{datos.anotador_id}", f"borrador:{tarea_id}",
        datos.token, datos.anotador_id, tarea_id, contenido,
    )
    if resultado is None:
        raise HTTPException(409, "El token o el candado no está vigente")

    tarea, etapa, borrador, vence = resultado
    return {
        "tarea_id": tarea_id,
        "tarea": json.loads(tarea),
        "etapa": etapa,
        "borrador": json.loads(borrador) if borrador else None,
        "vence_en": datetime.fromtimestamp(vence / 1000, timezone.utc),
    }


@app.put("/tareas/{tarea_id}/borrador")
async def guardar_borrador(tarea_id: str, request: Request, datos: BorradorIn):
    contenido = json.dumps({
        "datos": datos.borrador,
        "actualizado_en": datetime.now(timezone.utc).isoformat(),
    })
    return await operar_borrador(request, tarea_id, datos, contenido)


@app.post("/sesiones/recuperar")
async def recuperar_sesion(request: Request, datos: Propietario):
    tarea_id = await request.app.state.valkey.hget(f"sesion:{datos.anotador_id}", "tarea_id")
    if tarea_id is None:
        raise HTTPException(404, "El anotador no tiene una sesión activa")
    return await operar_borrador(request, tarea_id, datos)