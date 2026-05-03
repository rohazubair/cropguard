from __future__ import annotations
import sys
from pathlib import Path
from typing import Any
from fastapi import FastAPI
from fastapi.responses import RedirectResponse, Response
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
from ingestion.ingest_weather import _configure_search_path, connect_db
from api.dashboard_routes import router as dashboard_router

def _district_id_to_str(value: Any) -> str:
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)

def _connect():
    conn = connect_db()
    cur = conn.cursor()
    _configure_search_path(cur)
    return (conn, cur)
app = FastAPI(title='CropGuard Advisor', version='0.4.0')
app.include_router(dashboard_router)

@app.get('/')
def root() -> RedirectResponse:
    return RedirectResponse(url='/docs')

@app.get('/favicon.ico', include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)

@app.get('/healthz')
def healthz() -> dict[str, str]:
    return {'status': 'ok'}

@app.get('/districts')
def list_districts() -> dict[str, list[dict[str, Any]]]:
    conn, cur = _connect()
    try:
        cur.execute('\n            SELECT district_id, name, lat::float8, lon::float8\n            FROM bronze.districts_dim\n            ORDER BY name\n            ')
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    payload = []
    for row in rows:
        payload.append({'district_id': _district_id_to_str(row[0]), 'name': row[1], 'lat': row[2], 'lon': row[3]})
    return {'districts': payload}
