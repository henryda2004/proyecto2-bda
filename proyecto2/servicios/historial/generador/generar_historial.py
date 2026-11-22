
#!/usr/bin/env python3
"""
Generador de historial sintetico para el Proyecto 2.

Genera ciclos de vida completos de tareas y envia los eventos al
Servicio Historial mediante POST /eventos/lote.

Configuracion por defecto:
    100.000 tareas
    800.000 eventos
    12 meses
    25 % con 0 rechazos
    50 % con 1 rechazo
    25 % con 2 rechazos

Uso desde la raiz del repositorio:
    python servicios/historial/generador/generar_historial.py

Prueba pequena:
    python servicios/historial/generador/generar_historial.py --tareas 10

Simulacion sin enviar al servicio:
    python servicios/historial/generador/generar_historial.py --tareas 10 --dry-run
"""

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


# ---------------------------------------------------------------------
# Catalogo compartido
# ---------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[3]
DATASET_DIR = ROOT / "dataset"

sys.path.insert(0, str(DATASET_DIR))

from catalogo import SEMILLA, generar_catalogo, uid


# ---------------------------------------------------------------------
# Configuracion del historial
# ---------------------------------------------------------------------

URL_DEFAULT = "http://localhost:8003/eventos/lote"

INICIO_HISTORIAL = datetime(2025, 11, 22, tzinfo=timezone.utc)
FIN_HISTORIAL = datetime(2026, 11, 22, tzinfo=timezone.utc)

BATCHES_POR_PROYECTO = 5

MODALIDADES = {
    "texto": ["span"],
    "imagen": ["bbox", "poligono"],
    "video": ["bbox_video"],
    "lidar": ["cuboide_3d"],
}

MOTIVOS_RECHAZO = [
    "Etiquetado incompleto",
    "Clasificacion incorrecta",
    "Objeto omitido",
    "Bounding box incorrecto",
    "Taxonomia incorrecta",
    "Anotacion fuera de los criterios",
    "Requiere correccion de etiquetas",
]

# Ciclos:
# 0 rechazos = 4 eventos
# 1 rechazo   = 8 eventos
# 2 rechazos  = 12 eventos
#
# Distribucion exacta:
# 25 % / 50 % / 25 %
#
# Promedio: 8 eventos por tarea.


# ---------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------

def fecha_iso(fecha):
    return fecha.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def elegir_tipo_tarea(modalidad, rng):
    return rng.choice(MODALIDADES[modalidad])


def crear_fecha_inicio(proyecto, rng):
    """
    El evento inicial nunca ocurre antes de la creacion del proyecto.
    """

    minimo = max(INICIO_HISTORIAL, proyecto["creado_en"])

    # Evitamos iniciar tareas demasiado cerca del final del periodo,
    # para que sus ciclos de vida puedan completarse dentro del rango.
    maximo = FIN_HISTORIAL - timedelta(days=30)

    if minimo > maximo:
        minimo = maximo

    segundos = int((maximo - minimo).total_seconds())

    desplazamiento = rng.randint(0, max(0, segundos))

    return minimo + timedelta(seconds=desplazamiento)


def crear_distribucion_ciclos(cantidad, rng):
    """
    Distribuye las tareas entre los tres tipos de ciclo.

    El total de tareas se conserva exactamente. Si la cantidad no
    es divisible por cuatro, los residuos se asignan a los ciclos
    para aproximar 25/50/25.
    """

    n_cero = cantidad // 4
    n_uno = cantidad // 2
    n_dos = cantidad - n_cero - n_uno

    ciclos = (
        [0] * n_cero
        + [1] * n_uno
        + [2] * n_dos
    )

    rng.shuffle(ciclos)

    return ciclos


def crear_batches(proyectos, rng):
    """
    Crea IDs de batches historicos sinteticos por proyecto.
    """

    batches = {}

    for proyecto in proyectos:
        batches[proyecto["_id"]] = [
            uid(rng)
            for _ in range(BATCHES_POR_PROYECTO)
        ]

    return batches


def crear_grupos_anotadores(anotadores):
    """
    Separa anotadores y revisores y organiza los anotadores por equipo.
    """

    anotadores_normales = [
        a for a in anotadores if not a["es_revisor"]
    ]

    revisores = [
        a for a in anotadores if a["es_revisor"]
    ]

    por_equipo = defaultdict(list)

    for anotador in anotadores_normales:
        por_equipo[anotador["equipo_id"]].append(anotador)

    return anotadores_normales, revisores, por_equipo


# ---------------------------------------------------------------------
# Construccion de eventos
# ---------------------------------------------------------------------

def crear_evento(
    rng,
    tarea_id,
    version,
    estado,
    proyecto,
    batch_id,
    anotador,
    revisor_id,
    tipo_tarea,
    event_ts,
    duracion_seg=None,
    motivo_rechazo=None,
):
    """
    Construye un evento siguiendo el contrato de 16 columnas.
    """

    return {
        "event_id": uid(rng),
        "event_ts": fecha_iso(event_ts),
        "tarea_id": tarea_id,
        "version": version,
        "estado": estado,
        "cliente_id": proyecto["cliente_id"],
        "proyecto_id": proyecto["_id"],
        "batch_id": batch_id,
        "anotador_id": anotador["_id"],
        "equipo_id": anotador["equipo_id"],
        "revisor_id": revisor_id,
        "modalidad": proyecto["modalidad"],
        "tipo_tarea": tipo_tarea,
        "duracion_seg": duracion_seg,
        "motivo_rechazo": motivo_rechazo,
        "sla_horas": proyecto["sla_horas"],
    }


def generar_ciclo(
    rng,
    proyecto,
    batch_id,
    anotador_inicial,
    equipo_anotadores,
    revisores,
    cantidad_rechazos,
):
    """
    Genera un ciclo de vida completo de una tarea.

    Cada rechazo libera la tarea y provoca una nueva asignacion,
    representada por otro evento 'abierta'.
    """

    tarea_id = uid(rng)
    tipo_tarea = elegir_tipo_tarea(proyecto["modalidad"], rng)

    anotador_actual = anotador_inicial

    revisor = rng.choice(revisores)

    version = 1

    timestamp = crear_fecha_inicio(proyecto, rng)

    eventos = []

    # -------------------------------------------------------------
    # Primera asignacion
    # -------------------------------------------------------------

    eventos.append(
        crear_evento(
            rng=rng,
            tarea_id=tarea_id,
            version=version,
            estado="abierta",
            proyecto=proyecto,
            batch_id=batch_id,
            anotador=anotador_actual,
            revisor_id=None,
            tipo_tarea=tipo_tarea,
            event_ts=timestamp,
        )
    )

    # -------------------------------------------------------------
    # Ciclos de anotacion y revision
    # -------------------------------------------------------------

    for intento in range(cantidad_rechazos + 1):

        # Tiempo de anotacion
        timestamp += timedelta(
            hours=rng.uniform(1, 48)
        )

        duracion_seg = round(
            rng.uniform(30, 3600 * 4),
            2,
        )

        eventos.append(
            crear_evento(
                rng=rng,
                tarea_id=tarea_id,
                version=version,
                estado="anotada",
                proyecto=proyecto,
                batch_id=batch_id,
                anotador=anotador_actual,
                revisor_id=None,
                tipo_tarea=tipo_tarea,
                event_ts=timestamp,
                duracion_seg=duracion_seg,
            )
        )

        # Tiempo en cola de revision
        timestamp += timedelta(
            hours=rng.uniform(1, 72)
        )

        eventos.append(
            crear_evento(
                rng=rng,
                tarea_id=tarea_id,
                version=version,
                estado="en_revision",
                proyecto=proyecto,
                batch_id=batch_id,
                anotador=anotador_actual,
                revisor_id=revisor["_id"],
                tipo_tarea=tipo_tarea,
                event_ts=timestamp,
            )
        )

        # Tiempo que tarda el revisor en tomar una decision
        timestamp += timedelta(
            hours=rng.uniform(0.5, 48)
        )

        # ---------------------------------------------------------
        # Rechazo
        # ---------------------------------------------------------

        if intento < cantidad_rechazos:

            eventos.append(
                crear_evento(
                    rng=rng,
                    tarea_id=tarea_id,
                    version=version,
                    estado="rechazada",
                    proyecto=proyecto,
                    batch_id=batch_id,
                    anotador=anotador_actual,
                    revisor_id=revisor["_id"],
                    tipo_tarea=tipo_tarea,
                    event_ts=timestamp,
                    motivo_rechazo=rng.choice(MOTIVOS_RECHAZO),
                )
            )

            # Liberacion de la tarea.
            # "disponible" no produce evento.

            timestamp += timedelta(
                hours=rng.uniform(1, 24)
            )

            # Nueva asignacion, representada por "abierta".
            version += 1

            candidatos = [
                a for a in equipo_anotadores
                if a["_id"] != anotador_actual["_id"]
            ]

            if candidatos:
                anotador_actual = rng.choice(candidatos)

            eventos.append(
                crear_evento(
                    rng=rng,
                    tarea_id=tarea_id,
                    version=version,
                    estado="abierta",
                proyecto=proyecto,
                    batch_id=batch_id,
                    anotador=anotador_actual,
                    revisor_id=None,
                tipo_tarea=tipo_tarea,
                event_ts=timestamp,
            )
        )

        # ---------------------------------------------------------
        # Aprobacion final
        # ---------------------------------------------------------

        else:

            eventos.append(
                crear_evento(
                    rng=rng,
                    tarea_id=tarea_id,
                    version=version,
                    estado="aprobada",
                proyecto=proyecto,
                batch_id=batch_id,
                anotador=anotador_actual,
                revisor_id=revisor["_id"],
                tipo_tarea=tipo_tarea,
                event_ts=timestamp,
            )
        )

    return eventos


# ---------------------------------------------------------------------
# Envio HTTP
# ---------------------------------------------------------------------

def enviar_lote(url, eventos, intentos=3):
    """
    Envia un lote de eventos a POST /eventos/lote.

    Si se pierde una respuesta, el lote puede reenviarse. La
    deduplicacion del servicio Historial se encarga de event_id.
    """

    body = json.dumps(eventos).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
        },
        method="POST",
    )

    for intento in range(1, intentos + 1):

        try:

            with urllib.request.urlopen(
                request,
                timeout=120,
            ) as response:

                contenido = response.read().decode("utf-8")

                resultado = json.loads(contenido)

                if resultado.get("errores"):
                    raise RuntimeError(
                        f"Historial reporto errores: "
                        f"{resultado['errores'][:5]}"
                    )

                return resultado

        except (
            urllib.error.URLError,
            TimeoutError,
            RuntimeError,
        ) as exc:

            if intento == intentos:
                raise RuntimeError(
                    f"Fallo el envio despues de {intentos} "
                    f"intentos: {exc}"
                ) from exc

            espera = intento * 2

            print(
                f"  Error de envio: {exc}. "
                f"Reintentando en {espera}s..."
            )

            time.sleep(espera)


def enviar_flush(url):
    """
    Fuerza el flush de los eventos que quedaron pendientes.
    """

    base_url = url.split("/eventos", 1)[0].rstrip("/")
    flush_url = base_url + "/flush"

    request = urllib.request.Request(
        flush_url,
        data=b"",
        headers={
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(
        request,
        timeout=120,
    ) as response:

        return json.loads(
            response.read().decode("utf-8")
        )
# ---------------------------------------------------------------------
# Generacion principal
# ---------------------------------------------------------------------

def ejecutar(args):

    rng = random.Random(args.seed)

    catalogo = generar_catalogo(SEMILLA)

    proyectos = [
        p for p in catalogo["proyectos"]
        if p["estado"] == "activo"
    ]

    clientes = catalogo["clientes"]

    anotadores, revisores, por_equipo = crear_grupos_anotadores(
        catalogo["anotadores"]
    )

    batches = crear_batches(proyectos, rng)

    ciclos = crear_distribucion_ciclos(
        args.tareas,
        rng,
    )

    conteo_estados = Counter()
    conteo_modalidades = Counter()
    conteo_ciclos = Counter()
    conteo_meses = Counter()

    total_generados = 0
    total_enviados = 0
    total_lotes = 0

    buffer = []

    print("=" * 70)
    print("GENERADOR DE HISTORIAL")
    print("=" * 70)
    print(f"Tareas objetivo : {args.tareas:,}")
    print(f"Semilla        : {args.seed}")
    print(f"URL            : {args.url}")
    print(f"Tamano de lote : {args.batch_size}")
    print(f"Dry run        : {args.dry_run}")
    print("=" * 70)

    inicio = time.time()

    for i, cantidad_rechazos in enumerate(ciclos, start=1):

        proyecto = rng.choice(proyectos)

        batch_id = rng.choice(
            batches[proyecto["_id"]]
        )

        equipo_id = rng.choice(
            list(por_equipo.keys())
        )

        equipo_anotadores = por_equipo[equipo_id]

        anotador = rng.choice(equipo_anotadores)

        eventos = generar_ciclo(
            rng=rng,
            proyecto=proyecto,
            batch_id=batch_id,
            anotador_inicial=anotador,
            equipo_anotadores=equipo_anotadores,
            revisores=revisores,
            cantidad_rechazos=cantidad_rechazos,
        )

        conteo_ciclos[cantidad_rechazos] += 1
        conteo_modalidades[proyecto["modalidad"]] += len(eventos)

        for evento in eventos:

            total_generados += 1

            conteo_estados[evento["estado"]] += 1

            mes = evento["event_ts"][:7]
            conteo_meses[mes] += 1

            if args.dry_run:
                continue

            buffer.append(evento)

            if len(buffer) >= args.batch_size:

                resultado = enviar_lote(
                    args.url,
                    buffer,
                )

                total_enviados += resultado.get(
                    "procesados", len(buffer)
                )

                total_lotes += 1

                buffer.clear()

        # Reporte de progreso
        if i % args.progreso == 0 or i == args.tareas:

            elapsed = time.time() - inicio

            velocidad = (
                total_generados / elapsed
                if elapsed > 0 else 0
            )

            print(
                f"Tareas: {i:>8,}/{args.tareas:,} | "
                f"Eventos: {total_generados:>9,} | "
                f"Enviados: {total_enviados:>9,} | "
                f"Lotes: {total_lotes:>5,} | "
                f"{velocidad:,.0f} eventos/s"
            )

    # Enviar el lote final que no completo batch_size.
    if buffer and not args.dry_run:

        resultado = enviar_lote(
            args.url,
            buffer,
        )

        total_enviados += resultado.get(
            "procesados", len(buffer)
        )

        total_lotes += 1

        buffer.clear()

    # Flush final: procesa el remanente de SQLite.
    resultado_flush = None

    if not args.dry_run:

        print("\nEjecutando flush final...")

        resultado_flush = enviar_flush(args.url)

    elapsed = time.time() - inicio

    # -----------------------------------------------------------------
    # Resumen
    # -----------------------------------------------------------------

    print("\n" + "=" * 70)
    print("RESUMEN DE GENERACION")
    print("=" * 70)

    print(f"Tareas generadas : {args.tareas:,}")
    print(f"Eventos generados: {total_generados:,}")
    print(f"Eventos enviados : {total_enviados:,}")
    print(f"Lotes HTTP      : {total_lotes:,}")
    print(f"Tiempo total    : {elapsed:.2f} segundos")

    print("\nCiclos por cantidad de rechazos:")

    for rechazos in sorted(conteo_ciclos):

        print(
            f"  {rechazos} rechazos: "
            f"{conteo_ciclos[rechazos]:,} tareas"
        )

    print("\nEventos por estado:")

    for estado, cantidad in sorted(conteo_estados.items()):

        print(
            f"  {estado:12s} {cantidad:>10,}"
        )

    print("\nEventos por modalidad:")

    for modalidad, cantidad in sorted(conteo_modalidades.items()):

        print(
            f"  {modalidad:12s} {cantidad:>10,}"
        )

    print("\nEventos por mes:")

    for mes, cantidad in sorted(conteo_meses.items()):

        print(
            f"  {mes} {cantidad:>10,}"
        )

    if resultado_flush:

        print("\nResultado del flush final:")
        print(json.dumps(
            resultado_flush,
            indent=2,
            ensure_ascii=False,
        ))

    print("=" * 70)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="Genera historial sintetico y lo envia al servicio Historial."
    )

    parser.add_argument(
        "--tareas",
        type=int,
        default=100_000,
        help="Cantidad de tareas sinteticas (default: 100000)",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Cantidad de eventos por solicitud HTTP (default: 1000)",
    )

    parser.add_argument(
        "--url",
        default=URL_DEFAULT,
        help=f"Endpoint de Historial (default: {URL_DEFAULT})",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=SEMILLA + 100,
        help="Semilla para reproducibilidad",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Genera y cuenta eventos sin enviarlos al servicio",
    )

    parser.add_argument(
        "--progreso",
        type=int,
        default=1000,
        help="Mostrar progreso cada N tareas",
    )

    args = parser.parse_args()

    if args.tareas <= 0:
        parser.error("--tareas debe ser mayor que cero")

    if args.batch_size <= 0:
        parser.error("--batch-size debe ser mayor que cero")

    if args.progreso <= 0:
        parser.error("--progreso debe ser mayor que cero")

    ejecutar(args)


if __name__ == "__main__":
    main()
