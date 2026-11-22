import os
import sqlite3
import json
from contextlib import contextmanager

DB_PATH = os.getenv("HISTORIAL_DB", "/data/historial.db")


@contextmanager
def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eventos (
                event_id TEXT PRIMARY KEY,
                event_ts TEXT NOT NULL,
                mes TEXT NOT NULL,
                payload TEXT NOT NULL,
                procesado INTEGER NOT NULL DEFAULT 0
            )
        """)

        conn.execute("""
            DROP INDEX IF EXISTS idx_eventos_pendientes
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_eventos_pendientes
            ON eventos(procesado, event_ts, event_id)
        """)


def insertar_evento(evento: dict) -> bool:
    """
    Inserta un evento si su event_id no existe.

    Retorna:
        True: evento nuevo.
        False: evento duplicado.
    """
    with get_connection() as conn:
        cursor = conn.execute("""
            INSERT OR IGNORE INTO eventos (
                event_id,
                event_ts,
                mes,
                payload,
                procesado
            )
            VALUES (?, ?, ?, ?, 0)
        """, (
            evento["event_id"],
            evento["event_ts"],
            evento["mes"],
            json.dumps(evento["payload"], ensure_ascii=False),
        ))

        return cursor.rowcount == 1
    

def insertar_eventos(eventos: list[dict]) -> list[bool]:
    """
    Inserta múltiples eventos en una única conexión y transacción SQLite.

    Retorna una lista de booleanos, en el mismo orden:
        True: evento nuevo.
        False: evento duplicado.
    """
    resultados = []

    with get_connection() as conn:
        for evento in eventos:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO eventos (
                    event_id,
                    event_ts,
                    mes,
                    payload,
                    procesado
                )
                VALUES (?, ?, ?, ?, 0)
            """, (
                evento["event_id"],
                evento["event_ts"],
                evento["mes"],
                json.dumps(evento["payload"], ensure_ascii=False),
            ))

            resultados.append(cursor.rowcount == 1)

    return resultados

def obtener_pendientes(limite: int = 1000):
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT event_id, event_ts, mes, payload
            FROM eventos
            WHERE procesado = 0
            ORDER BY event_ts, event_id
            LIMIT ?
        """, (limite,)).fetchall()

        registros = []

        for row in rows:
            registro = dict(row)

            # payload se encuentra almacenado como JSON (texto)
            # en SQLite; aquí lo convertimos nuevamente a dict.
            registro["payload"] = json.loads(registro["payload"])

            registros.append(registro)

        return registros


def marcar_procesados(event_ids: list[str]):
    if not event_ids:
        return

    with get_connection() as conn:
        conn.executemany("""
            UPDATE eventos
            SET procesado = 1
            WHERE event_id = ?
        """, [(event_id,) for event_id in event_ids])


def contar_eventos():
    with get_connection() as conn:
        total = conn.execute("""
            SELECT COUNT(*) FROM eventos
        """).fetchone()[0]

        pendientes = conn.execute("""
            SELECT COUNT(*) FROM eventos
            WHERE procesado = 0
        """).fetchone()[0]

        procesados = conn.execute("""
            SELECT COUNT(*) FROM eventos
            WHERE procesado = 1
        """).fetchone()[0]

        return {
            "total": total,
            "pendientes": pendientes,
            "procesados": procesados,
        }
