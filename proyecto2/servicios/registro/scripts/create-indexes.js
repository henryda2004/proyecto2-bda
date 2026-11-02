const database = db.getSiblingDB("plataforma");

database.tareas.createIndex(
  { proyecto_id: 1, estado: 1, prioridad: 1 },
  { name: "idx_tareas_proyecto_estado_prioridad" }
);

database.tareas.createIndex(
  { "asignacion.anotador_id": 1, estado: 1 },
  { name: "idx_tareas_anotador_estado" }
);

database.tareas.createIndex(
  { batch_id: 1 },
  { name: "idx_tareas_batch" }
);

database.proyectos.createIndex(
  { cliente_id: 1, estado: 1 },
  { name: "idx_proyectos_cliente_estado" }
);

print(database.tareas.getIndexes().map((i) => i.name));
print(database.proyectos.getIndexes().map((i) => i.name));