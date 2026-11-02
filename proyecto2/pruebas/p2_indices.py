"""Justificacion de los indices del registro.

Para cada consulta real del servicio compara el plan normal (con indice) contra un
recorrido completo forzado de la coleccion, y guarda los resultados en evidence/.
"""
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"

JS = """
const d = db.getSiblingDB('plataforma');
const ahora = new Date();
const t = d.tareas.findOne({ estado: 'disponible' });
const proyecto = t.proyecto_id;
const lote = t.batch_id;
const cliente = d.proyectos.findOne({ _id: proyecto }).cliente_id;
const anotador = d.anotadores.findOne({ es_revisor: false })._id;

function indices(nodo, acc) {
  if (nodo && typeof nodo === 'object') {
    if (nodo.indexName) acc.push(nodo.indexName);
    Object.values(nodo).forEach(v => indices(v, acc));
  }
  return acc;
}

function medir(nombre, coleccion, filtro, orden, limite, forzar) {
  let c = d[coleccion].find(filtro);
  if (orden) c = c.sort(orden);
  if (limite) c = c.limit(limite);
  if (forzar) c = c.hint({ $natural: 1 });
  const e = c.explain('executionStats');
  const s = e.executionStats;
  return {
    consulta: nombre,
    modo: forzar ? 'recorrido completo forzado' : 'plan normal',
    indices_usados: indices(e.queryPlanner.winningPlan, []),
    recorrido_completo: JSON.stringify(e.queryPlanner.winningPlan).includes('COLLSCAN'),
    claves_examinadas: s.totalKeysExamined,
    documentos_examinados: s.totalDocsExamined,
    devueltos: s.nReturned,
    ms: s.executionTimeMillis
  };
}

const consultas = [
  ['siguiente tarea', 'tareas',
    { proyecto_id: proyecto, $or: [ { estado: 'disponible' }, { estado: 'abierta', 'asignacion.vence_en': { $lt: ahora } } ] },
    { prioridad: -1 }, 1],
  ['tarea abierta de un anotador', 'tareas', { 'asignacion.anotador_id': anotador, estado: 'abierta' }, null, null],
  ['tareas de un lote', 'tareas', { batch_id: lote }, null, null],
  ['proyectos activos de un cliente', 'proyectos', { cliente_id: cliente, estado: 'activo' }, null, null]
];

const resultados = [];
consultas.forEach(q => {
  resultados.push(medir(q[0], q[1], q[2], q[3], q[4], false));
  resultados.push(medir(q[0], q[1], q[2], q[3], q[4], true));
});

print(JSON.stringify({
  tamanos: { tareas: d.tareas.estimatedDocumentCount(), proyectos: d.proyectos.estimatedDocumentCount() },
  resultados: resultados
}));
"""


def main():
    inicio = datetime.now(timezone.utc)
    salida = subprocess.run(
        ["docker", "exec", "mongo1", "mongosh", MONGO_URI, "--quiet", "--eval", " ".join(JS.split())],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    datos = json.loads(salida)

    evidencia = {
        "fecha_hora_utc": inicio.isoformat(),
        "objetivo": "Justificar cada indice comparando el plan con indice contra un recorrido completo de la coleccion",
        "comando": "python proyecto2/pruebas/p2_indices.py",
        "tamanos": datos["tamanos"],
        "resultados": datos["resultados"],
    }
    destino = Path(__file__).resolve().parent.parent / "evidence" / "p2-indices-explain.json"
    destino.write_text(json.dumps(evidencia, indent=2), encoding="utf-8")

    print("tamanos", datos["tamanos"])
    for r in datos["resultados"]:
        print(
            f"{r['consulta']:33s} {r['modo']:27s} "
            f"docs {r['documentos_examinados']:6d} claves {r['claves_examinadas']:6d} "
            f"ms {r['ms']:4d} {r['indices_usados']}"
        )
    print("Evidencia guardada en", destino)


if __name__ == "__main__":
    main()