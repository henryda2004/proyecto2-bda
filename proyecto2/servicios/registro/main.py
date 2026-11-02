from fastapi import FastAPI
from fastapi.responses import JSONResponse

from db import client

app = FastAPI(title="Servicio de registro")


@app.get("/salud")
def salud():
    try:
        client.admin.command("ping")
        return {"servicio": "registro", "estado": "operativo"}
    except Exception:
        return JSONResponse(
            status_code=503,
            content={"servicio": "registro", "estado": "caido"},
        )