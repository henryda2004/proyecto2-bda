"""P1 (parte del registro). Evidencia de aislamiento modular y exposicion de puertos.

Comprueba que el codigo del registro no menciona Valkey, que solo el servicio registro
tiene configuracion de MongoDB y que puertos publica cada servicio con y sin el archivo
de desarrollo. Se puede volver a correr cuando se agreguen mas servicios.
Salida: proyecto2/evidence/p1-aislamiento-registro.json
"""
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
REGISTRO = RAIZ / "servicios" / "registro"


def buscar_valkey():
    coincidencias = []
    for ruta in sorted(REGISTRO.rglob("*")):
        if not ruta.is_file() or "__pycache__" in ruta.parts:
            continue
        if ruta.suffix not in {".py", ".txt", ".js", ""}:
            continue
        try:
            texto = ruta.read_text(encoding="utf-8")
        except Exception:
            continue
        for n, linea in enumerate(texto.splitlines(), 1):
            if re.search(r"valkey|redis", linea, re.I):
                coincidencias.append(f"{ruta.relative_to(RAIZ.parent).as_posix()}:{n}: {linea.strip()}")
    return coincidencias


def compose(*archivos):
    cmd = ["docker", "compose"]
    for a in archivos:
        cmd += ["-f", str(RAIZ / a)]
    cmd += ["config", "--format", "json"]
    r = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(r.stdout)


def resumen(cfg):
    publicados, con_mongo = {}, []
    for nombre, s in cfg["services"].items():
        pubs = [
            f"{p.get('host_ip', '0.0.0.0')}:{p['published']}->{p['target']}"
            for p in s.get("ports", [])
        ]
        if pubs:
            publicados[nombre] = pubs
        env = s.get("environment") or {}
        if any("mongo" in str(k).lower() or "mongo" in str(v).lower() for k, v in env.items()):
            con_mongo.append(nombre)
    return publicados, sorted(con_mongo)


def main():
    inicio = datetime.now(timezone.utc)
    coincidencias = buscar_valkey()
    base = compose("docker-compose.yml")
    dev = compose("docker-compose.yml", "docker-compose.dev.yml")
    puertos_base, con_mongo = resumen(base)
    puertos_dev, _ = resumen(dev)

    superado = not coincidencias and con_mongo == ["registro"] and set(puertos_base) <= {"gateway"}
    evidencia = {
        "fecha_hora_utc": inicio.isoformat(),
        "objetivo": "Mostrar que el registro no consulta Valkey, que solo el registro tiene configuracion de MongoDB y que puertos se publican",
        "comando": "python proyecto2/pruebas/p1_aislamiento_registro.py",
        "revision_codigo_registro": {
            "patron": "valkey|redis (sin distinguir mayusculas)",
            "carpeta": "proyecto2/servicios/registro",
            "coincidencias": coincidencias,
        },
        "servicios_con_configuracion_de_mongodb": con_mongo,
        "puertos_publicados_docker_compose_base": puertos_base,
        "puertos_publicados_con_docker_compose_dev": puertos_dev,
        "criterio": "sin menciones a Valkey, solo registro con configuracion de MongoDB, y en el compose base solo el gateway publica puertos",
        "prueba_superada": superado,
    }
    destino = RAIZ / "evidence" / "p1-aislamiento-registro.json"
    destino.write_text(json.dumps(evidencia, indent=2), encoding="utf-8")

    print("Menciones a Valkey en el registro", len(coincidencias))
    print("Servicios con configuracion de MongoDB", con_mongo)
    print("Puertos publicados en el compose base", puertos_base)
    print("Puertos publicados con el compose de desarrollo", puertos_dev)
    print("Prueba superada", superado)
    print("Evidencia guardada en", destino)
    raise SystemExit(0 if superado else 1)


if __name__ == "__main__":
    main()