import time
from datetime import date, datetime

from fastapi import FastAPI, HTTPException
from pyspark.sql import functions as F

from consultas import (
    obtener_spark,
    dataframe_eventos,
    limpiar_cache,
    consulta_q1,
    consulta_q2,
    consulta_q3,
    consulta_q4,
    consulta_q5,
    obtener_plan_q1,
    obtener_plan_q3,
)


app = FastAPI(
    title="Analítica - Proyecto 2",
    description="Consultas analíticas Q1-Q5 sobre el Datalake Parquet usando Spark",
    version="1.0.0",
)

@app.on_event("startup")
def inicializar_analitica():
    print("[Analitica] Iniciando preparación del dataset...", flush=True)

    df = dataframe_eventos()

    print("[Analitica] Dataset listo para consultas.", flush=True)

# ============================================================
# Serialización
# ============================================================

def serializar(valor):

    if isinstance(valor, (datetime, date)):
        return valor.isoformat()

    return valor


def dataframe_a_json(df):

    filas = df.collect()
    salida = []

    for fila in filas:
        registro = fila.asDict(recursive=True)

        registro = {
            key: serializar(value)
            for key, value in registro.items()
        }

        salida.append(registro)

    return salida


# ============================================================
# Ejecutor común
# ============================================================

def ejecutar_consulta(nombre, funcion):

    inicio = time.perf_counter()

    try:
        df = funcion()
        resultados = dataframe_a_json(df)

        elapsed = time.perf_counter() - inicio

        return {
            "consulta": nombre,
            "cantidad_resultados": len(resultados),
            "tiempo_segundos": round(elapsed, 4),
            "resultados": resultados,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error ejecutando {nombre}: {str(exc)}"
        )


# ============================================================
# Salud
# ============================================================

@app.get("/salud")
def salud():

    try:
        spark = obtener_spark()

        return {
            "servicio": "analitica",
            "estado": "operativo",
            "spark": "activo",
            "master": spark.sparkContext.master,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Spark no disponible: {str(exc)}"
        )


# ============================================================
# Estadísticas generales del Datalake
# ============================================================

@app.get("/estadisticas")
def estadisticas():

    try:
        df = dataframe_eventos()

        total = df.count()

        return {
            "eventos_unicos": total,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error leyendo el Datalake: {str(exc)}"
        )

# ============================================================
# Q1
# ============================================================

@app.get("/q1")
def q1():
    return ejecutar_consulta("Q1", consulta_q1)


# ============================================================
# Q2
# ============================================================

@app.get("/q2")
def q2():
    return ejecutar_consulta("Q2", consulta_q2)


# ============================================================
# Q3
# ============================================================

@app.get("/q3")
def q3():
    return ejecutar_consulta("Q3", consulta_q3)


# ============================================================
# Q4
# ============================================================

@app.get("/q4")
def q4():
    return ejecutar_consulta("Q4", consulta_q4)


# ============================================================
# Q5
# ============================================================

@app.get("/q5")
def q5():
    return ejecutar_consulta("Q5", consulta_q5)


# ============================================================
# Plan de ejecución Q1
# ============================================================

@app.get("/q1/plan")
def q1_plan():

    try:
        return {
            "consulta": "Q1",
            "plan": obtener_plan_q1(),
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo plan Q1: {str(exc)}"
        )


# ============================================================
# Plan de ejecución Q3
# ============================================================

@app.get("/q3/plan")
def q3_plan():

    try:
        return {
            "consulta": "Q3",
            "plan": obtener_plan_q3(),
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error obteniendo plan Q3: {str(exc)}"
        )


# ============================================================
# Actualizar lectura del Datalake
# ============================================================

@app.post("/actualizar")
def actualizar():
    print("[Analitica] Actualizando dataset...", flush=True)

    limpiar_cache()

    df = dataframe_eventos()

    cantidad = df.count()

    return {
        "estado": "ok",
        "mensaje": "Dataset actualizado y materializado",
        "eventos_unicos": cantidad
    }