from __future__ import annotations
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
import psycopg2
from fastapi import APIRouter, HTTPException
from psycopg2 import errorcodes
from psycopg2.extras import RealDictCursor
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
from ingestion.ingest_weather import connect_db
router = APIRouter(prefix='/dashboard', tags=['dashboard'])

def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value

def _fetch_rows(sql: str) -> list[dict[str, Any]]:
    conn = connect_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql)
                raw = cur.fetchall()
            except psycopg2.Error as exc:
                conn.rollback()
                if getattr(exc, 'pgcode', None) == errorcodes.UNDEFINED_TABLE:
                    raise HTTPException(status_code=503, detail='Gold dashboard tables are missing. Apply `storage/setup_db.sql` and run the forecast pipeline.') from exc
                raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()
    return [{k: _json_safe(v) for k, v in dict(row).items()} for row in raw]

@router.get('/published-run')
def dashboard_published_run() -> dict[str, list[dict[str, Any]]]:
    rows = _fetch_rows('SELECT * FROM gold.v_dashboard_published_forecast_run LIMIT 1')
    return {'rows': rows}

@router.get('/forecast-weather')
def dashboard_forecast_weather() -> dict[str, list[dict[str, Any]]]:
    rows = _fetch_rows('\n        SELECT *\n        FROM gold.v_dashboard_forecast_weather\n        ORDER BY district_name, valid_date, horizon_day\n        ')
    return {'rows': rows}

@router.get('/forecast-crop-risk')
def dashboard_forecast_crop_risk() -> dict[str, list[dict[str, Any]]]:
    rows = _fetch_rows('\n        SELECT *\n        FROM gold.v_dashboard_forecast_crop_risk\n        ORDER BY district_name, crop_name, valid_date\n        ')
    return {'rows': rows}
