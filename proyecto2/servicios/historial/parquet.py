import os
import hashlib
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from db import obtener_pendientes, marcar_procesados


DATA_LAKE = os.getenv("DATA_LAKE", "/datalake/eventos")

# Cantidad máxima de eventos que se extraen de SQLite por flush.
FLUSH_BATCH_SIZE = int(os.getenv("FLUSH_BATCH_SIZE", "10000"))


SCHEMA = pa.schema([
    pa.field("event_id", pa.string()),
    pa.field("event_ts", pa.timestamp("us", tz="UTC")),
    pa.field("tarea_id", pa.string()),
    pa.field("version", pa.int32()),
    pa.field("estado", pa.string()),
    pa.field("cliente_id", pa.string()),
    pa.field("proyecto_id", pa.string()),
    pa.field("batch_id", pa.string()),
    pa.field("anotador_id", pa.string()),
    pa.field("equipo_id", pa.string()),
    pa.field("revisor_id", pa.string()),
    pa.field("modalidad", pa.string()),
    pa.field("tipo_tarea", pa.string()),
    pa.field("duracion_seg", pa.float64()),
    pa.field("motivo_rechazo", pa.string()),
    pa.field("sla_horas", pa.int32()),
])


def escribir_grupo(mes: str, registros: list[dict]) -> str:
    """
    Escribe un grupo de eventos correspondiente a un mes.

    El nombre del archivo es determinístico según sus event_id.
    Esto permite reintentar una escritura después de un fallo
    sin generar otro archivo duplicado para el mismo grupo.
    """

    event_ids = sorted(r["event_id"] for r in registros)

    hash_lote = hashlib.sha256(
        "|".join(event_ids).encode("utf-8")
    ).hexdigest()[:16]

    directorio = os.path.join(DATA_LAKE, f"mes={mes}")
    os.makedirs(directorio, exist_ok=True)

    nombre = f"batch_{hash_lote}.parquet"
    destino = os.path.join(directorio, nombre)
    temporal = destino + ".tmp"

    # Si el archivo ya existe, la escritura de este lote
    # ya se completó en una ejecución anterior.
    if os.path.exists(destino):
        return destino

    rows = []

    for registro in registros:
        payload = registro["payload"]

        event_ts = datetime.fromisoformat(
            payload["event_ts"].replace("Z", "+00:00")
        )

        if event_ts.tzinfo is None:
            event_ts = event_ts.replace(tzinfo=timezone.utc)

        event_ts = event_ts.astimezone(timezone.utc)

        rows.append({
            "event_id": payload["event_id"],
            "event_ts": event_ts,
            "tarea_id": payload["tarea_id"],
            "version": payload["version"],
            "estado": payload["estado"],
            "cliente_id": payload["cliente_id"],
            "proyecto_id": payload["proyecto_id"],
            "batch_id": payload["batch_id"],
            "anotador_id": payload["anotador_id"],
            "equipo_id": payload["equipo_id"],
            "revisor_id": payload.get("revisor_id"),
            "modalidad": payload["modalidad"],
            "tipo_tarea": payload["tipo_tarea"],
            "duracion_seg": payload.get("duracion_seg"),
            "motivo_rechazo": payload.get("motivo_rechazo"),
            "sla_horas": payload["sla_horas"],
        })

    table = pa.Table.from_pylist(rows, schema=SCHEMA)

    pq.write_table(
        table,
        temporal,
        compression="snappy",
    )

    # Renombrado atómico dentro del mismo filesystem.
    os.replace(temporal, destino)

    return destino


def flush():
    """
    Procesa hasta FLUSH_BATCH_SIZE eventos pendientes de SQLite.

    Agrupa por mes, escribe Parquet y marca como procesados
    únicamente después de confirmar la existencia del archivo.
    """

    pendientes = obtener_pendientes(FLUSH_BATCH_SIZE)

    if not pendientes:
        return {
            "eventos_escritos": 0,
            "archivos": [],
            "mensaje": "No hay eventos pendientes",
        }

    grupos = defaultdict(list)

    for registro in pendientes:
        grupos[registro["mes"]].append(registro)

    archivos = []
    ids_procesados = []

    for mes, registros in grupos.items():
        archivo = escribir_grupo(mes, registros)

        # Confirmamos el procesamiento solamente si el archivo existe.
        if os.path.isfile(archivo):
            archivos.append(archivo)

            ids_procesados.extend(
                registro["event_id"]
                for registro in registros
            )

    marcar_procesados(ids_procesados)

    return {
        "eventos_escritos": len(ids_procesados),
        "archivos": archivos,
        "mensaje": "Flush completado",
    }


# ---------------------------------------------------------------------
# Compactacion
# ---------------------------------------------------------------------

import threading
import uuid

_COMPACTANDO = threading.Lock()


def compactar(minimo_archivos: int = 2) -> dict:
    """
    Fusiona los micro-lotes de cada particion mes=AAAA-MM en un solo archivo.

    Se leen los archivos existentes, se eliminan filas repetidas por event_id,
    se escribe el resultado en un .tmp y se publica con os.replace, igual que
    un micro-lote. Solo despues de publicar el archivo compactado se borran los
    archivos originales, de modo que un lector nunca ve la particion sin datos
    ni un archivo a medias. Los micro-lotes que lleguen durante la compactacion
    no se tocan porque no estaban en la lista inicial.
    """
    if not _COMPACTANDO.acquire(blocking=False):
        return {"mensaje": "Ya hay una compactacion en curso", "particiones": []}

    try:
        resumen = []
        if not os.path.isdir(DATA_LAKE):
            return {"mensaje": "Datalake vacio", "particiones": []}

        for nombre_dir in sorted(os.listdir(DATA_LAKE)):
            directorio = os.path.join(DATA_LAKE, nombre_dir)
            if not (nombre_dir.startswith("mes=") and os.path.isdir(directorio)):
                continue

            archivos = sorted(
                os.path.join(directorio, f)
                for f in os.listdir(directorio)
                if f.endswith(".parquet")
            )
            if len(archivos) < minimo_archivos:
                continue

            tablas = [pq.read_table(a, schema=SCHEMA) for a in archivos]
            tabla = pa.concat_tables(tablas)
            filas_entrada = tabla.num_rows

            vistos = set()
            indices = []
            for i, event_id in enumerate(tabla.column("event_id").to_pylist()):
                if event_id not in vistos:
                    vistos.add(event_id)
                    indices.append(i)
            tabla = tabla.take(pa.array(indices, type=pa.int64()))

            destino = os.path.join(
                directorio, f"compactado_{uuid.uuid4().hex[:16]}.parquet"
            )
            temporal = destino + ".tmp"
            pq.write_table(tabla, temporal, compression="snappy")
            os.replace(temporal, destino)

            for a in archivos:
                os.remove(a)

            resumen.append({
                "particion": nombre_dir,
                "archivos_entrada": len(archivos),
                "filas_entrada": filas_entrada,
                "filas_salida": tabla.num_rows,
                "archivo_salida": destino,
            })

        return {"mensaje": "Compactacion completada", "particiones": resumen}
    finally:
        _COMPACTANDO.release()
