"""Genera los datos del registro (MongoDB) como archivos JSONL en proyecto2/datos/registro/.

Volumen de referencia
  60 clientes, 120 proyectos activos, 600 lotes, 60000 tareas
  (30 % texto, 30 % imagen, 20 % video, 20 % LiDAR) y 300 anotadores en 30 equipos.

Uso
  python proyecto2/dataset/generar_registro.py
"""
import hashlib
import json
import random
from collections import Counter
from datetime import timedelta
from pathlib import Path

from catalogo import SEMILLA, generar_catalogo, uid

LOTES_POR_PROYECTO = 5
TAREAS_POR_LOTE = 100

EXT = {"texto": "txt", "imagen": "jpg", "video": "mp4", "lidar": "pcd"}
MIME = {
    "texto": "text/plain",
    "imagen": "image/jpeg",
    "video": "video/mp4",
    "lidar": "application/octet-stream",
}


def datos_de(mod, rng):
    if mod == "texto":
        c = rng.randint(200, 5000)
        return c * 2, {"caracteres": c, "idioma": "es"}
    if mod == "imagen":
        ancho = rng.choice([1280, 1920, 3840])
        alto = ancho * 9 // 16
        return ancho * alto // 6, {"ancho": ancho, "alto": alto}
    if mod == "video":
        seg = rng.randint(5, 60)
        return seg * 1_500_000, {"duracion_seg": seg, "fps": rng.choice([24, 30])}
    puntos = rng.randint(50_000, 150_000)
    return puntos * 16, {
        "puntos": puntos,
        "min": round(rng.uniform(-80, -20), 2),
        "max": round(rng.uniform(40, 150), 2),
    }


def tipo_de(mod, rng):
    if mod == "imagen":
        return rng.choice(["bbox", "poligono"])
    return {"texto": "span", "video": "bbox_video", "lidar": "cuboide_3d"}[mod]


def fecha_ej(o):
    return {"$date": o.strftime("%Y-%m-%dT%H:%M:%SZ")}


def escribir(ruta, docs):
    with ruta.open("w", encoding="utf-8", newline="\n") as f:
        for d in docs:
            f.write(json.dumps(d, default=fecha_ej, ensure_ascii=False) + "\n")


def main():
    destino = Path(__file__).resolve().parent.parent / "datos" / "registro"
    destino.mkdir(parents=True, exist_ok=True)

    cat = generar_catalogo()
    proyectos = [p for p in cat["proyectos"] if p["estado"] == "activo"]

    rng = random.Random(SEMILLA + 1)
    lotes, tareas = [], []
    for p in proyectos:
        mod = p["modalidad"]
        for k in range(1, LOTES_POR_PROYECTO + 1):
            lote_id = uid(rng)
            creado = p["creado_en"] + timedelta(days=k)
            lotes.append(
                {
                    "_id": lote_id,
                    "proyecto_id": p["_id"],
                    "nombre": f"lote-{k:02d}",
                    "total_tareas": TAREAS_POR_LOTE,
                    "creado_en": creado,
                }
            )
            for n in range(1, TAREAS_POR_LOTE + 1):
                uri = f"s3://anotacion-datos/{p['cliente_id']}/{p['_id']}/{lote_id}/{n:05d}.{EXT[mod]}"
                nbytes, meta = datos_de(mod, rng)
                tareas.append(
                    {
                        "_id": uid(rng),
                        "proyecto_id": p["_id"],
                        "batch_id": lote_id,
                        "cliente_id": p["cliente_id"],
                        "modalidad": mod,
                        "tipo_tarea": tipo_de(mod, rng),
                        "uri": uri,
                        "sha256": hashlib.sha256(uri.encode()).hexdigest(),
                        "bytes": nbytes,
                        "mime": MIME[mod],
                        "meta": meta,
                        "prioridad": rng.randint(0, 9),
                        "estado": "disponible",
                        "version": 0,
                        "asignacion": None,
                        "anotacion": None,
                        "ultima_revision": None,
                        "ultimo_evento_id": None,
                        "creado_en": creado,
                    }
                )

    escribir(destino / "clientes.jsonl", cat["clientes"])
    escribir(destino / "proyectos.jsonl", proyectos)
    escribir(destino / "batches.jsonl", lotes)
    escribir(destino / "tareas.jsonl", tareas)
    escribir(destino / "anotadores.jsonl", cat["anotadores"])

    print("Archivos en", destino)
    print("clientes  ", len(cat["clientes"]))
    print("proyectos ", len(proyectos))
    print("lotes     ", len(lotes))
    print("tareas    ", len(tareas))
    print("anotadores", len(cat["anotadores"]))
    print("revisores ", sum(1 for a in cat["anotadores"] if a["es_revisor"]))
    for m, n in sorted(Counter(t["modalidad"] for t in tareas).items()):
        print(f"  {m:7s} {n} ({100 * n / len(tareas):.0f} %)")


if __name__ == "__main__":
    main()