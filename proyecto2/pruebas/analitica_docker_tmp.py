
#!/usr/bin/env python3

"""
Validaciones y benchmarks de Analítica (Q1-Q5).

Modos:
    python pruebas/analitica_controlada.py validar
    python pruebas/analitica_controlada.py benchmark --repeticiones 5
    python pruebas/analitica_controlada.py workers --repeticiones 5

VALIDAR:
    Usa un dataset controlado en memoria.
    No lee ni modifica el Datalake.

BENCHMARK:
    Mide la primera ejecución y ejecuciones posteriores de Q1-Q5
    sobre el Datalake real.

WORKERS:
    Ejecuta el benchmark con SPARK_WORKERS=1 y SPARK_WORKERS=2
    en procesos independientes.
"""

import os
import sys
import json
import time
import math
import argparse
import subprocess
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


# Agregar raíz del repositorio al sys.path
ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


# Python explícito para Spark Driver y Workers en Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ============================================================
# Configuración
# ============================================================

CONSULTAS = [
    ("Q1", "consulta_q1"),
    ("Q2", "consulta_q2"),
    ("Q3", "consulta_q3"),
    ("Q4", "consulta_q4"),
    ("Q5", "consulta_q5"),
]

SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_ts", StringType(), False),
    StructField("tarea_id", StringType(), False),
    StructField("version", IntegerType(), False),
    StructField("estado", StringType(), False),
    StructField("cliente_id", StringType(), False),
    StructField("proyecto_id", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("anotador_id", StringType(), False),
    StructField("equipo_id", StringType(), False),
    StructField("revisor_id", StringType(), True),
    StructField("modalidad", StringType(), False),
    StructField("tipo_tarea", StringType(), False),
    StructField("duracion_seg", DoubleType(), True),
    StructField("motivo_rechazo", StringType(), True),
    StructField("sla_horas", DoubleType(), False),
])


# ============================================================
# Dataset controlado
# ============================================================

def crear_eventos_controlados():
    """
    Construye eventos artificiales para comprobar Q1-Q5.

    El dataset se mantiene exclusivamente en memoria.
    """

    eventos = []

    def agregar(
        event_id,
        fecha,
        tarea,
        estado,
        cliente="C0",
        proyecto="P0",
        equipo="E0",
        modalidad="texto",
        tipo="span",
        duracion=None,
        sla=24.0,
        motivo=None,
    ):
        eventos.append({
            "event_id": event_id,
            "event_ts": fecha,
            "tarea_id": tarea,
            "version": 1,
            "estado": estado,
            "cliente_id": cliente,
            "proyecto_id": proyecto,
            "batch_id": "B-TEST",
            "anotador_id": "A-TEST",
            "equipo_id": equipo,
            "revisor_id": "R-TEST",
            "modalidad": modalidad,
            "tipo_tarea": tipo,
            "duracion_seg": (
                float(duracion) if duracion is not None else None
            ),
            "motivo_rechazo": motivo,
            "sla_horas": float(sla),
        })

    # --------------------------------------------------------
    # Q1 - Aprobadas por día y modalidad
    #
    # Último evento del dataset: 2026-01-31.
    # Ventana: últimos 90 días relativos a esa fecha.
    #
    # 2026-01-30 texto: 2 tareas únicas (T-Q1-01 y T-Q1-02)
    # 2026-01-31 imagen: 1 tarea (T-Q1-03)
    # Evento duplicado de aprobación para T-Q1-01:
    # countDistinct debe evitar contarla dos veces.
    # --------------------------------------------------------

    agregar(
        "Q1-01", "2026-01-30T10:00:00Z",
        "T-Q1-01", "aprobada",
        modalidad="texto"
    )

    agregar(
        "Q1-02", "2026-01-30T11:00:00Z",
        "T-Q1-01", "aprobada",
        modalidad="texto"
    )

    agregar(
        "Q1-03", "2026-01-30T12:00:00Z",
        "T-Q1-02", "aprobada",
        modalidad="texto"
    )

    agregar(
        "Q1-04", "2026-01-31T09:00:00Z",
        "T-Q1-03", "aprobada",
        modalidad="imagen",
        tipo="box"
    )

    agregar(
        "Q1-05", "2025-10-01T09:00:00Z",
        "T-Q1-04", "aprobada",
        modalidad="texto"
    )

    # --------------------------------------------------------
    # Q2 - Ratio de aprobación por equipo y mes
    #
    # E-Q2-01 / 2026-01:
    # revisadas: 2 tareas
    # aprobadas: 2 tareas
    # ratio esperado: 1.0
    #
    # E-Q2-02 / 2026-01:
    # revisadas: 3 tareas
    # aprobadas: 1 tarea
    # ratio esperado: 1/3
    # --------------------------------------------------------

    for tarea in ("T-Q2-A1", "T-Q2-A2"):
        agregar(
            f"{tarea}-R", "2026-01-10T10:00:00Z",
            tarea, "en_revision",
            equipo="E-Q2-01"
        )

        agregar(
            f"{tarea}-AP", "2026-01-11T10:00:00Z",
            tarea, "aprobada",
            equipo="E-Q2-01"
        )

    for tarea in ("T-Q2-B1", "T-Q2-B2", "T-Q2-B3"):
        agregar(
            f"{tarea}-R", "2026-01-15T10:00:00Z",
            tarea, "en_revision",
            equipo="E-Q2-02"
        )

    agregar(
        "T-Q2-B1-AP", "2026-01-16T10:00:00Z",
        "T-Q2-B1", "aprobada",
        equipo="E-Q2-02"
    )

    # --------------------------------------------------------
    # Q3 - Duración media y P95
    #
    # texto/span: [10, 20, 30]
    # media esperada: 20
    # p95 esperado: 30
    #
    # imagen/box: [100, 200]
    # media esperada: 150
    # p95 esperado: 200
    # --------------------------------------------------------

    for i, duracion in enumerate([10, 20, 30], start=1):
        agregar(
            f"Q3-TEXT-{i}",
            f"2026-01-{20+i:02d}T10:00:00Z",
            f"T-Q3-TEXT-{i}",
            "anotada",
            modalidad="texto",
            tipo="span",
            duracion=duracion
        )

    for i, duracion in enumerate([100, 200], start=1):
        agregar(
            f"Q3-IMAGE-{i}",
            f"2026-01-{25+i:02d}T10:00:00Z",
            f"T-Q3-IMAGE-{i}",
            "anotada",
            modalidad="imagen",
            tipo="box",
            duracion=duracion
        )

    # Anotación sin duración: debe excluirse de Q3.
    agregar(
        "Q3-NULL",
        "2026-01-29T10:00:00Z",
        "T-Q3-NULL",
        "anotada",
        modalidad="texto",
        tipo="span",
        duracion=None
    )

    # --------------------------------------------------------
    # Q4 - Rechazos por cliente y proyecto
    #
    # C-Q4-01 / P-Q4-01:
    # revisadas 3, rechazadas 1 -> 1/3
    #
    # C-Q4-01 / P-Q4-02:
    # revisadas 2, rechazadas 2 -> 1.0
    #
    # C-Q4-02 / P-Q4-03:
    # revisadas 2, rechazadas 0 -> 0.0
    # --------------------------------------------------------

    for tarea in ("T-Q4-A1", "T-Q4-A2", "T-Q4-A3"):
        agregar(
            f"{tarea}-REV",
            "2026-01-05T10:00:00Z",
            tarea, "en_revision",
            cliente="C-Q4-01",
            proyecto="P-Q4-01"
        )

    agregar(
        "T-Q4-A1-REJ",
        "2026-01-06T10:00:00Z",
        "T-Q4-A1", "rechazada",
        cliente="C-Q4-01",
        proyecto="P-Q4-01",
        motivo="TEST"
    )

    for tarea in ("T-Q4-B1", "T-Q4-B2"):
        agregar(
            f"{tarea}-REV",
            "2026-01-07T10:00:00Z",
            tarea, "en_revision",
            cliente="C-Q4-01",
            proyecto="P-Q4-02"
        )

        agregar(
            f"{tarea}-REJ",
            "2026-01-08T10:00:00Z",
            tarea, "rechazada",
            cliente="C-Q4-01",
            proyecto="P-Q4-02",
            motivo="TEST"
        )

    for tarea in ("T-Q4-C1", "T-Q4-C2"):
        agregar(
            f"{tarea}-REV",
            "2026-01-09T10:00:00Z",
            tarea, "en_revision",
            cliente="C-Q4-02",
            proyecto="P-Q4-03"
        )

    # --------------------------------------------------------
    # Q5 - Tiempo entre última toma y aprobación / SLA
    #
    # Cliente C-Q5-01:
    # Duraciones [2, 4, 8] horas
    # P50 = 4, P95 = 8
    # SLA [3, 3, 10] horas
    # Incumplimientos: 1/3 -> 33.333...%
    #
    # Cliente C-Q5-02:
    # Duración [10] horas
    # P50 = 10, P95 = 10
    # SLA [5]
    # Incumplimiento: 100%
    # --------------------------------------------------------

    casos_q5 = [
        ("T-Q5-A1", "2026-01-01T00:00:00Z", 2, 3),
        ("T-Q5-A2", "2026-01-02T00:00:00Z", 4, 3),
        ("T-Q5-A3", "2026-01-03T00:00:00Z", 8, 10),
        ("T-Q5-B1", "2026-01-04T00:00:00Z", 10, 5),
    ]

    for tarea, inicio, horas, sla in casos_q5:
        cliente = (
            "C-Q5-01"
            if tarea.startswith("T-Q5-A")
            else "C-Q5-02"
        )

        fecha_inicio = datetime.fromisoformat(
            inicio.replace("Z", "+00:00")
        )

        fecha_aprobacion = (
            fecha_inicio.timestamp() + horas * 3600
        )

        fecha_aprobacion = datetime.fromtimestamp(
            fecha_aprobacion,
            tz=timezone.utc
        ).isoformat().replace("+00:00", "Z")

        agregar(
            f"{tarea}-OPEN",
            inicio,
            tarea, "abierta",
            cliente=cliente,
            proyecto="P-Q5",
            sla=sla
        )

        agregar(
            f"{tarea}-APP",
            fecha_aprobacion,
            tarea, "aprobada",
            cliente=cliente,
            proyecto="P-Q5",
            sla=sla
        )

    # Convertimos los timestamps string a timestamp Spark.
    return eventos


# ============================================================
# Validación funcional
# ============================================================

def validar_resultado(nombre, actual, esperado, tolerancia=0.001):

    if isinstance(esperado, float):
        correcto = (
            actual is not None
            and math.isclose(
                float(actual),
                esperado,
                rel_tol=tolerancia,
                abs_tol=tolerancia
            )
        )
    else:
        correcto = actual == esperado

    estado = "PASS" if correcto else "FAIL"

    print(
        f"[{estado}] {nombre}: "
        f"esperado={esperado}, obtenido={actual}"
    )

    if not correcto:
        raise AssertionError(
            f"{nombre}: esperado {esperado}, obtenido {actual}"
        )


def ejecutar_validacion():

    print("=" * 70)
    print("VALIDACIÓN CONTROLADA Q1-Q5")
    print("=" * 70)

    spark = (
        SparkSession.builder
        .appName("Analitica-Validacion-Controlada")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    import consultas

    eventos_raw = crear_eventos_controlados()

    df = spark.createDataFrame(eventos_raw, schema=SCHEMA)

    df = (
        df.withColumn(
            "event_ts",
            F.to_timestamp("event_ts")
        )
    )

    # Inyección del dataset de prueba.
    consultas.dataframe_eventos = lambda: df

    try:

        # ----------------------------------------------------
        # Q1
        # ----------------------------------------------------

        print("\n--- Q1 ---")

        q1 = consultas.consulta_q1().collect()

        q1_resultados = {
            (
                fila["dia"].strftime("%Y-%m-%d"),
                fila["modalidad"]
            ): fila["tareas_aprobadas"]
            for fila in q1
        }

        validar_resultado(
            "Q1 2026-01-30 texto",
            q1_resultados.get(("2026-01-30", "texto")),
            2
        )

        validar_resultado(
            "Q1 2026-01-31 imagen",
            q1_resultados.get(("2026-01-31", "imagen")),
            1
        )

        validar_resultado(
            "Q1 excluye fecha fuera de ventana",
            q1_resultados.get(("2025-10-01", "texto")),
            None
        )

        # ----------------------------------------------------
        # Q2
        # ----------------------------------------------------

        print("\n--- Q2 ---")

        q2 = consultas.consulta_q2().collect()

        q2_resultados = {
            (fila["equipo_id"], fila["mes"]): fila
            for fila in q2
        }

        e1 = q2_resultados[("E-Q2-01", "2026-01")]

        validar_resultado(
            "Q2 E-Q2-01 revisadas",
            e1["tareas_revisadas"],
            2
        )

        validar_resultado(
            "Q2 E-Q2-01 aprobadas",
            e1["tareas_aprobadas"],
            2
        )

        validar_resultado(
            "Q2 E-Q2-01 ratio",
            e1["ratio_aprobacion"],
            1.0
        )

        e2 = q2_resultados[("E-Q2-02", "2026-01")]

        validar_resultado(
            "Q2 E-Q2-02 revisadas",
            e2["tareas_revisadas"],
            3
        )

        validar_resultado(
            "Q2 E-Q2-02 aprobadas",
            e2["tareas_aprobadas"],
            1
        )

        validar_resultado(
            "Q2 E-Q2-02 ratio",
            e2["ratio_aprobacion"],
            1 / 3
        )

        # ----------------------------------------------------
        # Q3
        # ----------------------------------------------------

        print("\n--- Q3 ---")

        q3 = consultas.consulta_q3().collect()

        q3_resultados = {
            (fila["modalidad"], fila["tipo_tarea"]): fila
            for fila in q3
        }

        texto = q3_resultados[("texto", "span")]

        validar_resultado(
            "Q3 texto/span cantidad",
            texto["cantidad_anotaciones"],
            3
        )

        validar_resultado(
            "Q3 texto/span media",
            texto["duracion_media_seg"],
            20.0
        )

        validar_resultado(
            "Q3 texto/span P95",
            texto["duracion_p95_seg"],
            30.0
        )

        imagen = q3_resultados[("imagen", "box")]

        validar_resultado(
            "Q3 imagen/box cantidad",
            imagen["cantidad_anotaciones"],
            2
        )

        validar_resultado(
            "Q3 imagen/box media",
            imagen["duracion_media_seg"],
            150.0
        )

        validar_resultado(
            "Q3 imagen/box P95",
            imagen["duracion_p95_seg"],
            200.0
        )

        # ----------------------------------------------------
        # Q4
        # ----------------------------------------------------

        print("\n--- Q4 ---")

        q4 = consultas.consulta_q4().collect()

        q4_resultados = {
            (fila["cliente_id"], fila["proyecto_id"]): fila
            for fila in q4
        }

        a = q4_resultados[("C-Q4-01", "P-Q4-01")]

        validar_resultado(
            "Q4 C-Q4-01/P-Q4-01 revisadas",
            a["tareas_revisadas"],
            3
        )

        validar_resultado(
            "Q4 C-Q4-01/P-Q4-01 rechazadas",
            a["tareas_rechazadas"],
            1
        )

        validar_resultado(
            "Q4 C-Q4-01/P-Q4-01 ratio",
            a["ratio_rechazo"],
            1 / 3
        )

        b = q4_resultados[("C-Q4-01", "P-Q4-02")]

        validar_resultado(
            "Q4 C-Q4-01/P-Q4-02 ratio",
            b["ratio_rechazo"],
            1.0
        )

        c = q4_resultados[("C-Q4-02", "P-Q4-03")]

        validar_resultado(
            "Q4 C-Q4-02/P-Q4-03 rechazadas",
            c["tareas_rechazadas"],
            0
        )

        validar_resultado(
            "Q4 C-Q4-02/P-Q4-03 ratio",
            c["ratio_rechazo"],
            0.0
        )

        # ----------------------------------------------------
        # Q5
        # ----------------------------------------------------

        print("\n--- Q5 ---")

        q5 = consultas.consulta_q5().collect()

        q5_resultados = {
            fila["cliente_id"]: fila
            for fila in q5
        }

        cliente_a = q5_resultados["C-Q5-01"]

        validar_resultado(
            "Q5 C-Q5-01 cantidad",
            cliente_a["tareas_aprobadas"],
            3
        )

        validar_resultado(
            "Q5 C-Q5-01 P50",
            cliente_a["tiempo_p50_horas"],
            4.0
        )

        validar_resultado(
            "Q5 C-Q5-01 P95",
            cliente_a["tiempo_p95_horas"],
            8.0
        )

        validar_resultado(
            "Q5 C-Q5-01 incumplimiento SLA %",
            cliente_a["porcentaje_incumplimiento_sla"],
            100 / 3
        )

        cliente_b = q5_resultados["C-Q5-02"]

        validar_resultado(
            "Q5 C-Q5-02 P50",
            cliente_b["tiempo_p50_horas"],
            10.0
        )

        validar_resultado(
            "Q5 C-Q5-02 P95",
            cliente_b["tiempo_p95_horas"],
            10.0
        )

        validar_resultado(
            "Q5 C-Q5-02 incumplimiento SLA %",
            cliente_b["porcentaje_incumplimiento_sla"],
            100.0
        )

        print("\n" + "=" * 70)
        print("VALIDACIÓN COMPLETADA: TODAS LAS PRUEBAS PASS")
        print("=" * 70)

    finally:
        consultas.dataframe_eventos = consultas.obtener_eventos
        spark.stop()


# ============================================================
# Benchmark: primera ejecución vs. posteriores
# ============================================================

def ejecutar_benchmark(repeticiones=5):

    import consultas

    print("=" * 70)
    print("BENCHMARK Q1-Q5")
    print(f"SPARK_WORKERS={os.getenv('SPARK_WORKERS', '1')}")
    print(f"Repeticiones posteriores={repeticiones}")
    print("=" * 70)

    # Inicialización y materialización del dataset se miden aparte.
    inicio_preparacion = time.perf_counter()

    df = consultas.obtener_eventos()
    cantidad_eventos = df.count()

    tiempo_preparacion = time.perf_counter() - inicio_preparacion

    print(
        f"\nPreparación dataset: {tiempo_preparacion:.4f} s"
    )
    print(f"Eventos únicos: {cantidad_eventos}")

    resultados = {}

    for nombre, funcion_nombre in CONSULTAS:

        funcion = getattr(consultas, funcion_nombre)

        tiempos = []
        cantidad_resultados = None

        for i in range(repeticiones + 1):

            inicio = time.perf_counter()

            resultado = funcion()

            filas = resultado.collect()

            tiempo = time.perf_counter() - inicio

            tiempos.append(tiempo)

            if cantidad_resultados is None:
                cantidad_resultados = len(filas)

            etiqueta = (
                "Primera"
                if i == 0
                else f"Posterior {i}"
            )

            print(
                f"{nombre} | {etiqueta}: "
                f"{tiempo:.4f} s | filas={len(filas)}"
            )

        posteriores = tiempos[1:]

        promedio_posterior = (
            sum(posteriores) / len(posteriores)
            if posteriores
            else None
        )

        resultados[nombre] = {
            "primera_s": tiempos[0],
            "posteriores_s": posteriores,
            "promedio_posterior_s": promedio_posterior,
            "filas": cantidad_resultados,
        }

    reporte = {
        "workers": int(os.getenv("SPARK_WORKERS", "1")),
        "eventos": cantidad_eventos,
        "preparacion_s": tiempo_preparacion,
        "consultas": resultados,
    }

    print("\nRESULTADO_JSON")
    print(json.dumps(reporte, indent=2))

    return reporte


# ============================================================
# Comparación: 1 worker vs. 2 workers
# ============================================================

def ejecutar_comparacion_workers(repeticiones=5):

    print("=" * 70)
    print("COMPARACIÓN SPARK 1 WORKER vs 2 WORKERS")
    print("=" * 70)

    script = os.path.abspath(__file__)

    reportes = {}

    for workers in (1, 2):

        print(f"\n{'=' * 70}")
        print(f"Ejecutando benchmark con SPARK_WORKERS={workers}")
        print("=" * 70)

        env = os.environ.copy()
        env["SPARK_WORKERS"] = str(workers)

        comando = [
            sys.executable,
            script,
            "benchmark",
            "--repeticiones",
            str(repeticiones),
        ]

        proceso = subprocess.run(
            comando,
            env=env,
            capture_output=True,
            text=True,
        )

        print(proceso.stdout)

        if proceso.returncode != 0:
            print(proceso.stderr)
            raise RuntimeError(
                f"Benchmark falló con {workers} workers"
            )

        salida = proceso.stdout

        marcador = "RESULTADO_JSON\n"

        if marcador not in salida:
            raise RuntimeError(
                "No se encontró RESULTADO_JSON en la salida."
            )

        json_texto = salida.split(marcador, 1)[1].strip()

        reportes[str(workers)] = json.loads(json_texto)

    # --------------------------------------------------------
    # Comparación
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("RESUMEN COMPARATIVO")
    print("=" * 70)

    print(
        f"{'Query':<8}"
        f"{'1 worker':>15}"
        f"{'2 workers':>15}"
        f"{'Speedup':>15}"
    )

    for nombre, _ in CONSULTAS:

        t1 = reportes["1"]["consultas"][nombre][
            "promedio_posterior_s"
        ]

        t2 = reportes["2"]["consultas"][nombre][
            "promedio_posterior_s"
        ]

        speedup = (
            t1 / t2
            if t2 and t2 > 0
            else float("nan")
        )

        print(
            f"{nombre:<8}"
            f"{t1:>15.4f}"
            f"{t2:>15.4f}"
            f"{speedup:>14.2f}x"
        )

    print("\nPreparación inicial del dataset:")

    print(
        "1 worker: "
        f"{reportes['1']['preparacion_s']:.4f} s"
    )

    print(
        "2 workers: "
        f"{reportes['2']['preparacion_s']:.4f} s"
    )

    print("\nEventos procesados:")

    print(
        "1 worker: "
        f"{reportes['1']['eventos']}"
    )

    print(
        "2 workers: "
        f"{reportes['2']['eventos']}"
    )


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Validación y benchmarking de Analítica"
    )

    parser.add_argument(
        "modo",
        choices=["validar", "benchmark", "workers"]
    )

    parser.add_argument(
        "--repeticiones",
        type=int,
        default=5,
        help="Cantidad de ejecuciones posteriores por consulta"
    )

    args = parser.parse_args()

    if args.repeticiones < 1:
        parser.error("--repeticiones debe ser >= 1")

    if args.modo == "validar":
        ejecutar_validacion()

    elif args.modo == "benchmark":
        ejecutar_benchmark(args.repeticiones)

    elif args.modo == "workers":
        ejecutar_comparacion_workers(args.repeticiones)


if __name__ == "__main__":
    main()
