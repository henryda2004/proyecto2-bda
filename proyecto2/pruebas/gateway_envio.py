"""Prueba la confirmación del envío y el rechazo del duplicado."""

from hashlib import sha256

from sesion_basica import llamar


def probar():
    anotador = "ana-prueba-gateway"
    proyecto = llamar("POST", "/proyectos", {
        "cliente_id": "cli-prueba-gateway",
        "nombre": "Prueba de envío Gateway",
        "modalidad": "texto",
        "taxonomia": [{"nombre": "persona", "instruccion": "Marcar nombres"}],
    }, 201)["id"]

    texto = b"Ana escribe."
    llamar("POST", f"/proyectos/{proyecto}/lotes", {
        "nombre": "Lote de prueba de envío",
        "tareas": [{
            "uri": "s3://pruebas-gateway/envio.txt",
            "sha256": sha256(texto).hexdigest(),
            "bytes": len(texto),
            "mime": "text/plain",
            "tipo_tarea": "span",
        }],
    }, 201)

    asignada = llamar("POST", "/tareas/siguiente", {
        "proyecto_id": proyecto, "anotador_id": anotador,
    }, 200)
    ruta = f"/tareas/{asignada['tarea']['id']}"
    propietario = {"anotador_id": anotador, "token": asignada["token"]}
    anotacion = {"spans": [{"inicio": 0, "fin": 3, "etiqueta": "persona"}]}
    datos = {**propietario, "anotacion": anotacion}

    confirmacion = None
    try:
        llamar("POST", ruta + "/enviar", {**datos, "token": "incorrecto"}, 409)
        confirmacion = llamar("POST", ruta + "/enviar", datos, 200)
    finally:
        if confirmacion is None:
            try:
                llamar("POST", ruta + "/liberar", propietario, 200)
            except (OSError, RuntimeError):
                pass

    assert confirmacion["estado"] == "anotada"
    assert confirmacion["evento"]["estado"] == "anotada"
    llamar("POST", ruta + "/enviar", datos, 409)
    llamar("POST", "/sesiones/recuperar", propietario, 404)
    print("Verificado. Envío confirmado, segundo envío rechazado y sesión cerrada.")


if __name__ == "__main__":
    probar()