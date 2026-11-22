
import os
from functools import lru_cache
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATA_LAKE = os.getenv("DATA_LAKE", "/datalake/eventos")
SPARK_WORKERS = int(os.getenv("SPARK_WORKERS", "1"))


# ============================================================
# Spark
# ============================================================

@lru_cache(maxsize=1)
def obtener_spark():
    spark = (
        SparkSession.builder
        .appName("Proyecto2-Analitica")
        .master(f"local[{SPARK_WORKERS}]")
        .config("spark.sql.shuffle.partitions", str(SPARK_WORKERS))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# Datalake
# ============================================================

@lru_cache(maxsize=1)
def obtener_eventos():
    spark = obtener_spark()

    print("[Analitica] Leyendo Datalake Parquet...", flush=True)

    df = (
        spark.read
        .option("basePath", DATA_LAKE)
        .parquet(DATA_LAKE)
    )

    print("[Analitica] Eliminando duplicados...", flush=True)

    df = df.dropDuplicates(["event_id"])

    print("[Analitica] Persistiendo dataset en MEMORY_AND_DISK...", flush=True)

    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    print("[Analitica] Materializando dataset...", flush=True)

    cantidad = df.count()

    print(f"[Analitica] Dataset preparado: {cantidad} eventos únicos.", flush=True)

    return df


def dataframe_eventos():
    return obtener_eventos()


def limpiar_cache():
    if obtener_eventos.cache_info().currsize > 0:
        df = obtener_eventos()
        df.unpersist(blocking=True)

    obtener_eventos.cache_clear()

    obtener_spark().catalog.clearCache()

# ============================================================
# Helpers
# ============================================================

def plan_consulta(df, modo="formatted"):
    return df._jdf.queryExecution().explainString(modo)


# ============================================================
# Q1 - Aprobadas por día y modalidad, últimos 90 días
# ============================================================

def consulta_q1():

    eventos = dataframe_eventos()

    fecha_max = (
        eventos
        .agg(F.max("event_ts").alias("fecha_max"))
        .first()["fecha_max"]
    )

    if fecha_max is None:
        return eventos.limit(0)

    fecha_limite = (
        F.lit(fecha_max) - F.expr("INTERVAL 90 DAYS")
    )

    return (
        eventos
        .filter(F.col("estado") == "aprobada")
        .filter(F.col("event_ts") >= fecha_limite)
        .groupBy(
            F.to_date("event_ts").alias("dia"),
            "modalidad"
        )
        .agg(
            F.countDistinct("tarea_id").alias("tareas_aprobadas")
        )
        .orderBy("dia", "modalidad")
    )


# ============================================================
# Q2 - Ratio de aprobación por equipo y mes
# ============================================================

def consulta_q2():

    eventos = dataframe_eventos()

    revisadas = (
        eventos
        .filter(F.col("estado") == "en_revision")
        .groupBy(
            "equipo_id",
            F.date_format("event_ts", "yyyy-MM").alias("mes")
        )
        .agg(
            F.countDistinct("tarea_id").alias("tareas_revisadas")
        )
    )

    aprobadas = (
        eventos
        .filter(F.col("estado") == "aprobada")
        .groupBy(
            "equipo_id",
            F.date_format("event_ts", "yyyy-MM").alias("mes")
        )
        .agg(
            F.countDistinct("tarea_id").alias("tareas_aprobadas")
        )
    )

    return (
        revisadas
        .join(aprobadas, ["equipo_id", "mes"], "left")
        .fillna(0, subset=["tareas_aprobadas"])
        .withColumn(
            "ratio_aprobacion",
            F.col("tareas_aprobadas") /
            F.col("tareas_revisadas")
        )
        .orderBy("mes", "equipo_id")
    )


# ============================================================
# Q3 - Duración media y p95 por modalidad y tipo de tarea
# ============================================================

def consulta_q3():

    eventos = dataframe_eventos()

    return (
        eventos
        .filter(F.col("estado") == "anotada")
        .filter(F.col("duracion_seg").isNotNull())
        .groupBy("modalidad", "tipo_tarea")
        .agg(
            F.count("*").alias("cantidad_anotaciones"),
            F.avg("duracion_seg").alias("duracion_media_seg"),
            F.percentile_approx(
                "duracion_seg", 0.95, 10000
            ).alias("duracion_p95_seg")
        )
        .orderBy("modalidad", "tipo_tarea")
    )


# ============================================================
# Q4 - Ratio de rechazo por cliente y proyecto
# ============================================================

def consulta_q4():

    eventos = dataframe_eventos()

    revisadas = (
        eventos
        .filter(F.col("estado") == "en_revision")
        .groupBy("cliente_id", "proyecto_id")
        .agg(
            F.countDistinct("tarea_id").alias("tareas_revisadas")
        )
    )

    rechazadas = (
        eventos
        .filter(F.col("estado") == "rechazada")
        .groupBy("cliente_id", "proyecto_id")
        .agg(
            F.countDistinct("tarea_id").alias("tareas_rechazadas")
        )
    )

    return (
        revisadas
        .join(rechazadas, ["cliente_id", "proyecto_id"], "left")
        .fillna(0, subset=["tareas_rechazadas"])
        .withColumn(
            "ratio_rechazo",
            F.col("tareas_rechazadas") /
            F.col("tareas_revisadas")
        )
        .orderBy("cliente_id", "proyecto_id")
    )


# ============================================================
# Q5 - Tiempo desde última toma hasta aprobación
# p50, p95 y porcentaje de incumplimiento SLA por cliente
# ============================================================

def consulta_q5():

    eventos = dataframe_eventos()

    # La última asignación "abierta" es la toma del intento
    # que culmina con la aprobación.
    tomas = (
        eventos
        .filter(F.col("estado") == "abierta")
        .groupBy("tarea_id")
        .agg(
            F.max("event_ts").alias("fecha_toma")
        )
    )

    aprobaciones = (
        eventos
        .filter(F.col("estado") == "aprobada")
        .groupBy("tarea_id")
        .agg(
            F.min("event_ts").alias("fecha_aprobacion"),
            F.first("cliente_id", ignorenulls=True).alias("cliente_id"),
            F.first("sla_horas", ignorenulls=True).alias("sla_horas")
        )
    )

    ciclos = (
        tomas
        .join(aprobaciones, "tarea_id", "inner")
        .withColumn(
            "tiempo_horas",
            (
                F.col("fecha_aprobacion").cast("long")
                - F.col("fecha_toma").cast("long")
            ) / 3600.0
        )
        .withColumn(
            "incumple_sla",
            F.col("tiempo_horas") > F.col("sla_horas")
        )
    )

    return (
        ciclos
        .groupBy("cliente_id")
        .agg(
            F.count("*").alias("tareas_aprobadas"),
            F.percentile_approx(
                "tiempo_horas", 0.50, 10000
            ).alias("tiempo_p50_horas"),
            F.percentile_approx(
                "tiempo_horas", 0.95, 10000
            ).alias("tiempo_p95_horas"),
            (
                F.avg(F.col("incumple_sla").cast("double")) * 100
            ).alias("porcentaje_incumplimiento_sla")
        )
        .orderBy("cliente_id")
    )


# ============================================================
# Planes de ejecución Q1 y Q3
# ============================================================

def obtener_plan_q1():
    return plan_consulta(consulta_q1())


def obtener_plan_q3():
    return plan_consulta(consulta_q3())
