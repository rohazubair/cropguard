import sys

sys.dont_write_bytecode = True

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
import requests
from psycopg2 import sql
from psycopg2.extras import execute_batch

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_a, **_kw):  # pragma: no cover
        return False

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

BRONZE_SCHEMA = "bronze"

OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = "temperature_2m,relative_humidity_2m"

WEATHER_INSERT_SQL = """
    INSERT INTO bronze.weather_readings_fact (
        district_id,
        observed_ts,
        temperature,
        humidity_pct
    ) VALUES (%s, %s, %s, %s)
    ON CONFLICT (district_id, observed_ts) DO NOTHING
    """


def _database_url():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Add it to .env in the cropguard folder or export it in your environment."
        )
    return url.strip().strip('"')


def _configure_search_path(cur):
    cur.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(BRONZE_SCHEMA))
    )


def connect_db(*, attempts: int = 8, initial_delay_s: float = 2.0) -> psycopg2.extensions.connection:
    for attempt in range(attempts):
        try:
            return psycopg2.connect(_database_url())
        except psycopg2.OperationalError as exc:
            msg = str(exc).lower()
            if "password authentication failed" in msg or "no pg_hba.conf entry" in msg:
                raise
            if attempt == attempts - 1:
                raise
            time.sleep(min(initial_delay_s * (2**attempt), 45.0))

    raise RuntimeError("connect_db: unreachable")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc_aware(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _parse_observed_ts(ts_raw: Any) -> datetime:
    return pd.to_datetime(ts_raw, utc=True).to_pydatetime()


def get_latest_observed_ts(district_id: int) -> datetime | None:
    """CDC anchor: latest stored hour for this district (stored as UTC wall time, naive)."""
    conn = connect_db()
    try:
        cur = conn.cursor()
        _configure_search_path(cur)
        cur.execute(
            """
            SELECT MAX(observed_ts)
            FROM bronze.weather_readings_fact
            WHERE district_id = %s
            """,
            (district_id,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()
    if not row or row[0] is None:
        return None
    return _as_utc_aware(row[0])


def get_districts():
    conn = connect_db()
    try:
        cur = conn.cursor()
        _configure_search_path(cur)
        cur.execute(
            """
            SELECT district_id, name, lat, lon
            FROM bronze.districts_dim
            ORDER BY district_id
            """
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    return [
        {
            "district_id": row[0],
            "name": row[1],
            "lat": float(row[2]),
            "lon": float(row[3]),
        }
        for row in rows
    ]


def fetch_weather(
    lat: float,
    lon: float,
    *,
    past_days: int = 14,
    forecast_days: int = 7,
    max_observed_ts: datetime | None = None,
) -> dict[str, Any]:
    """
    Open-Meteo hourly forecast (UTC).

    - No rows in DB yet for this run context: use past_days + forecast_days (historical + forecast backfill).
    - Rows already present (CDC): fetch only from today's date through today + forecast_days; past in DB
      is left unchanged. New hours are still filtered against MAX(observed_ts) before insert.
    """
    params: dict[str, Any] = {
        "latitude": lat,
        "longitude": lon,
        "hourly": HOURLY_VARS,
        "timezone": "UTC",
    }
    if max_observed_ts is not None:
        today = _utc_now().date()
        end_d = today + timedelta(days=forecast_days)
        params["start_date"] = today.isoformat()
        params["end_date"] = end_d.isoformat()
    else:
        params["past_days"] = past_days
        params["forecast_days"] = forecast_days

    response = requests.get(OPEN_METEO_FORECAST, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


def _hourly_series(hourly: dict[str, Any], key: str) -> list[Any] | None:
    v = hourly.get(key)
    return v if isinstance(v, list) else None


def save_to_db(district: dict[str, Any], *, past_days: int = 14, forecast_days: int = 7) -> dict[str, Any]:
    """
    If the district has no weather rows yet: Open-Meteo past_days + forecast_days (past is loaded once).

    If rows exist: Open-Meteo only from today (UTC) through today + forecast_days; existing past rows stay
    as-is. Inserts only hours strictly after MAX(observed_ts) (ON CONFLICT still guards duplicates).
    """
    district_id = int(district["district_id"])
    lat = float(district["lat"])
    lon = float(district["lon"])

    max_ts = get_latest_observed_ts(district_id)
    payload = fetch_weather(
        lat,
        lon,
        past_days=past_days,
        forecast_days=forecast_days,
        max_observed_ts=max_ts,
    )

    hourly = payload.get("hourly") or {}
    times = _hourly_series(hourly, "time")
    temps = _hourly_series(hourly, "temperature_2m")
    humid = _hourly_series(hourly, "relative_humidity_2m")

    if not times or not temps or not humid:
        return {
            "district_id": district_id,
            "hours_insert_attempted": 0,
            "hours_in_response": 0,
            "hours_skipped_cdc": 0,
            "cdc_anchor_utc": max_ts.isoformat() if max_ts else None,
            "note": "missing hourly arrays in API response",
        }

    n = min(len(times), len(temps), len(humid))
    rows: list[tuple[Any, ...]] = []
    skipped_cdc = 0

    for i in range(n):
        t_val = temps[i]
        h_val = humid[i]
        if t_val is None or h_val is None:
            continue
        observed = _parse_observed_ts(times[i])
        if max_ts is not None and observed <= _as_utc_aware(max_ts):
            skipped_cdc += 1
            continue
        hum = int(round(float(h_val)))
        hum = max(0, min(100, hum))
        rows.append(
            (
                district_id,
                observed.replace(tzinfo=None),
                float(t_val),
                hum,
            )
        )

    conn = connect_db()
    try:
        cur = conn.cursor()
        _configure_search_path(cur)
        if rows:
            execute_batch(cur, WEATHER_INSERT_SQL, rows, page_size=500)
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "district_id": district_id,
        "hours_insert_attempted": len(rows),
        "hours_in_response": n,
        "hours_skipped_cdc": skipped_cdc,
        "cdc_anchor_utc": max_ts.isoformat() if max_ts else None,
    }


if __name__ == "__main__":
    districts = get_districts()
    if not districts:
        print("No rows in bronze.districts_dim. From the cropguard folder run: python main.py")
    for d in districts:
        print(f"Ingesting data for {d['name']}...")
        print(save_to_db(d))
    print("Ingestion complete!")
