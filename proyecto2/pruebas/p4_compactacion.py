"""P4. Compactacion de micro-lotes del datalake.

Cuenta archivos y eventos unicos antes y despues de POST /compactar del
historial, y verifica con Spark (POST /actualizar) que el numero de eventos
unicos no cambia.

Uso, desde proyecto2/:
    python pruebas/p4_compactacion.py

Salida: evidence/p4-compactacion-<fecha>.json
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

HISTORIAL = "http://127.0.0.1:8003"
ANALITICA = "http://127.0.0.1:8004"
RAIZ = Path(__file__).resolve().parents[1]
DATALAKE = RAIZ / "datalake" / "eventos"


def http(metodo, url):
    solicitud = Request(url, data=b"{}" if metodo == "POST" else None, method=metodo,
                        headers={"Content-Type": "application/json"})
    try:
        respuesta = urlopen(solicitud, timeout=600)
    except HTTPError as error:
        respuesta = error
    with respuesta:
        return respuesta.status, json.loads(respuesta.read() or b"{}")


def inventario():
    particiones = {}
    for d in sorted(DATALAKE.glob("mes=*")):
        particiones[d.name] = len(list(d.glob("*.parquet")))
    return {"archivos_por_particion": particiones,
            "archivos_total": sum(particiones.values())}


def main():
    ahora = datetime.now(timezone.utc)
    antes = inventario()
    print("Archivos antes:", antes["archivos_total"])
    _, spark_antes = http("POST", f"{ANALITICA}/actualizar")
    print("Eventos unicos en Spark antes:", spark_antes.get("eventos_unicos"))

    codigo, compactacion = http("POST", f"{HISTORIAL}/compactar")
    print("POST /compactar ->", codigo)

    despues = inventario()
    print("Archivos despues:", despues["archivos_total"])
    _, spark_despues = http("POST", f"{ANALITICA}/actualizar")
    print("Eventos unicos en Spark despues:", spark_despues.get("eventos_unicos"))

    superada = (codigo == 200
                and spark_antes.get("eventos_unicos") == spark_despues.get("eventos_unicos")
                and despues["archivos_total"] <= antes["archivos_total"])
    salida = RAIZ / "evidence" / f"p4-compactacion-{ahora.strftime('%Y%m%dT%H%M%SZ')}.json"
    salida.write_text(json.dumps({
        "fecha_hora_utc": ahora.isoformat(),
        "objetivo": "Fusionar los micro-lotes de cada particion mes=AAAA-MM sin perder ni "
                    "duplicar eventos y sin exponer archivos a medias.",
        "comando": "python pruebas/p4_compactacion.py",
        "antes": {**antes, "eventos_unicos_spark": spark_antes.get("eventos_unicos")},
        "compactacion_http": codigo,
        "compactacion": compactacion,
        "despues": {**despues, "eventos_unicos_spark": spark_despues.get("eventos_unicos")},
        "prueba_superada": superada,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Prueba superada:", superada)
    print("Evidencia guardada en", salida)


if __name__ == "__main__":
    main()
