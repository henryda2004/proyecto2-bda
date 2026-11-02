const database = db.getSiblingDB("plataforma");

const collections = ["clientes", "proyectos", "batches", "tareas", "anotadores"];
const existing = database.getCollectionNames();

collections.forEach((name) => {
  if (!existing.includes(name)) {
    database.createCollection(name);
  }
});

print(database.getCollectionNames());