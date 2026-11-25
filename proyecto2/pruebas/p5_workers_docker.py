"""P5. Uno contra dos trabajadores de Spark dentro de Docker.

Reinicia el contenedor de analitica con SPARK_WORKERS=1 y luego con 2, mide el
tiempo hasta que /salud queda operativo (incluye leer, deduplicar y cachear el
datalake) y ejecuta Q1 a Q5 una primera vez y cinco veces mas. Al final deja
la analitica de nuevo con 1 trabajador.

Uso, desde proyecto2/:
    python pruebas/p5_workers_docker.py

Salida: evidence/p5-workers-docker-<fecha>.json
"""

import json
import os
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ANALITICA = "http://127.0.0.1:8004"
RAIZ = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose",
           "-f", "docker-compose.yml", "-f", "docker-compose.dev.yml",
           "-f", "docker-compose.gateway.yml", "-f", "docker-compose.historial.yml",
           "-f", "docker-compose.analitica.yml"]
POSTERIORES = 5


def get(ruta, timeout=600):
    with urlopen(Request(ANALITICA + ruta), timeout=timeout) as r:
        return json.loads(r.read())


def levantar(workers):
    entorno = dict(os.environ, SPARK_WORKERS=str(workers))
    subprocess.run(COMPOSE + ["up", "-d", "--force-recreate", "analitica"],
                   cwd=RAIZ, env=entorno, check=True, capture_output=True)
    inicio = time.perf_counter()
    while time.perf_counter() - inicio < 600:
        try:
            salud = get("/salud", timeout=5)
            if salud.get("estado") == "operativo":
                return round(time.perf_counter() - inicio, 2), salud
        except (URLError, HTTPError, OSError, ValueError):
            pass
        time.sleep(1)
    raise RuntimeError("analitica no quedo operativa")


def medir():
    consultas = {}
    for q in ("q1", "q2", "q3", "q4", "q5"):
        tiempos = []
        filas = None
        for _ in range(1 + POSTERIORES):
            inicio = time.perf_counter()
            r = get(f"/{q}")
            pared = time.perf_counter() - inicio
            tiempos.append({"servidor_s": r.get("tiempo_segundos"), "pared_s": round(pared, 4)})
            filas = r.get("cantidad_resultados")
        posteriores = [t["servidor_s"] for t in tiempos[1:]]
        consultas[q.upper()] = {
            "primera_s": tiempos[0]["servidor_s"],
            "posteriores_s": posteriores,
            "promedio_posterior_s": round(statistics.mean(posteriores), 4),
            "filas": filas,
            "detalle": tiempos,
        }
        print(f"  {q.upper()}: primera {tiempos[0]['servidor_s']} s, "
              f"promedio posterior {consultas[q.upper()]['promedio_posterior_s']} s, filas {filas}")
    return consultas


def main():
    ahora = datetime.now(timezone.utc)
    resultados = {}
    for workers in (1, 2):
        print(f"SPARK_WORKERS={workers}")
        arranque, salud = levantar(workers)
        estadisticas = get("/estadisticas")
        print(f"  operativa en {arranque} s ({salud.get('master')})")
        resultados[f"local[{workers}]"] = {
            "arranque_hasta_operativo_s": arranque,
            "master": salud.get("master"),
            "estadisticas": estadisticas,
            "consultas": medir(),
        }
    print("Restaurando SPARK_WORKERS=1")
    levantar(1)

    salida = RAIZ / "evidence" / f"p5-workers-docker-{ahora.strftime('%Y%m%dT%H%M%SZ')}.json"
    salida.write_text(json.dumps({
        "fecha_hora_utc": ahora.isoformat(),
        "objetivo": "Comparar uno y dos trabajadores de Spark dentro del contenedor de "
                    "analitica, separando el arranque de la primera ejecucion y de las "
                    "ejecuciones posteriores de Q1 a Q5.",
        "comando": "python pruebas/p5_workers_docker.py",
        "configuracion": {"posteriores": POSTERIORES,
                          "medida": "tiempo_segundos que reporta el servicio por consulta"},
        "resultados": resultados,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Evidencia guardada en", salida)


if __name__ == "__main__":
    main()
