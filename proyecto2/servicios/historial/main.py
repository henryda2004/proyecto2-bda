import os
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

import db
import parquet


BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
FLUSH_BATCH_SIZE = int(os.getenv("FLUSH_BATCH_SIZE", "10000"))

app = FastAPI(
    title="Servicio Historial",
    description="Recepción, validación y persistencia de eventos de tareas.",
    version="1.0.0",
)


class Estado(str, Enum):
    abierta = "abierta"
    anotada = "anotada"
    en_revision = "en_revision"
    aprobada = "aprobada"
    rechazada = "rechazada"


class Modalidad(str, Enum):
    texto = "texto"
    imagen = "imagen"
    video = "video"
    lidar = "lidar"


class TipoTarea(str, Enum):
    span = "span"
    bbox = "bbox"
    poligono = "poligono"
    bbox_video = "bbox_video"
    cuboide_3d = "cuboide_3d"


class Evento(BaseModel):
    event_id: str
    event_ts: datetime
    tarea_id: str
    version: int = Field(ge=1)
    estado: Estado

    cliente_id: str
    proyecto_id: str
    batch_id: str
    anotador_id: str
    equipo_id: str
    revisor_id: Optional[str] = None

    modalidad: Modalidad
    tipo_tarea: TipoTarea

    duracion_seg: Optional[float] = Field(default=None, ge=0)
    motivo_rechazo: Optional[str] = None

    sla_horas: int = Field(gt=0)

    @model_validator(mode="after")
    def validar_consistencia(self):
        if self.estado != Estado.anotada and self.duracion_seg is not None:
            raise ValueError(
                "duracion_seg solo se permite en estado anotada"
            )

        if self.estado != Estado.rechazada and self.motivo_rechazo is not None:
            raise ValueError(
                "motivo_rechazo solo se permite en estado rechazada"
            )

        return self


def normalizar_evento(evento: Evento):
    """
    Normaliza la fecha a UTC y prepara el evento para SQLite.
    """

    ts = evento.event_ts

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    ts = ts.astimezone(timezone.utc)

    payload = evento.model_dump(mode="json")
    payload["event_ts"] = ts.isoformat().replace("+00:00", "Z")

    return {
        "event_id": payload["event_id"],
        "event_ts": payload["event_ts"],
        "mes": ts.strftime("%Y-%m"),
        "payload": payload,
    }


def procesar_evento(evento: Evento):
    registro = normalizar_evento(evento)

    nuevo = db.insertar_evento(registro)

    resultado = {
        "event_id": evento.event_id,
        "recibido": True,
        "duplicado": not nuevo,
    }

    # Si llegamos al umbral de microbatch, escribimos.
    estadisticas = db.contar_eventos()

    if estadisticas["pendientes"] >= FLUSH_BATCH_SIZE:
        resultado["flush"] = parquet.flush()

    return resultado


@app.on_event("startup")
def startup():
    db.init_db()

    # Recuperación de eventos pendientes de ejecuciones anteriores.
    # Si había eventos guardados en SQLite y el contenedor se
    # reinició, intentamos persistirlos en Parquet.
    try:
        parquet.flush()
    except Exception as exc:
        print(f"Error durante flush inicial: {exc}")


@app.get("/salud")
def salud():
    return {
        "servicio": "historial",
        "estado": "operativo",
    }


@app.post("/eventos")
def recibir_evento(evento: Evento):
    try:
        return procesar_evento(evento)

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando evento: {str(exc)}",
        )


@app.post("/eventos/lote")
def recibir_lote(eventos: list[Evento]):
    if not eventos:
        raise HTTPException(
            status_code=400,
            detail="La lista de eventos está vacía"
        )

    try:
        # Normalizar todos los eventos recibidos
        registros = [
            normalizar_evento(evento)
            for evento in eventos
        ]

        # Insertar el lote en una única conexión/transacción SQLite
        inserciones = db.insertar_eventos(registros)

        resultados = [
            {
                "event_id": evento.event_id,
                "recibido": True,
                "duplicado": not nuevo,
            }
            for evento, nuevo in zip(eventos, inserciones)
        ]

        # Consultar estadísticas únicamente una vez por lote
        estadisticas = db.contar_eventos()

        resultado = {
            "recibidos": len(eventos),
            "procesados": len(eventos),
            "errores": [],
            "resultados": resultados,
        }

        # Evaluar el flush una sola vez por lote
        if estadisticas["pendientes"] >= FLUSH_BATCH_SIZE:
            resultado["flush"] = parquet.flush()

        return resultado

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando lote: {str(exc)}"
        )

@app.post("/flush")
def forzar_flush():
    try:
        return parquet.flush()

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error escribiendo Parquet: {str(exc)}",
        )


@app.get("/estadisticas")
def estadisticas():
    return db.contar_eventos()
