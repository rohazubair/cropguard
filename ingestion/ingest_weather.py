import sys

sys.dont_write_bytecode = True

import os
from pathlib import Path

import psycopg2
from psycopg2 import sql
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def _database_url():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Add it to .env in the cropguard folder or export it in your environment."
        )
    return url.strip().strip('"')


def _database_schema():
    return os.environ.get("DATABASE_SCHEMA", "").strip()


def _configure_search_path(cur):
    schema = _database_schema()
    if not schema:
        return
    cur.execute(
        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
    )


def get_districts():
    conn = psycopg2.connect(_database_url())
    try:
        cur = conn.cursor()
        _configure_search_path(cur)
        cur.execute(
            """
            SELECT district_id, name, province, lat, lon
            FROM DimDistrict
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
            "province": row[2],
            "lat": float(row[3]),
            "lon": float(row[4]),
        }
        for row in rows
    ]


def fetch_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,relative_humidity_2m,soil_moisture_0_to_7cm",
        "timezone": "auto",
        "forecast_days": 1,
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def save_to_db(district_id, data):
    conn = psycopg2.connect(_database_url())
    try:
        cur = conn.cursor()
        _configure_search_path(cur)
        hourly = data["hourly"]
        for i in range(len(hourly["time"])):
            cur.execute(
                """
                INSERT INTO FactWeatherReadings (district_id, timestamp, temp_min, precipitation_mm, humidity_pct, soil_moisture)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (district_id, timestamp) DO NOTHING;
                """,
                (
                    district_id,
                    hourly["time"][i],
                    hourly["temperature_2m"][i],
                    hourly["precipitation"][i],
                    hourly["relative_humidity_2m"][i],
                    hourly["soil_moisture_0_to_7cm"][i],
                ),
            )
        conn.commit()
        cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    districts = get_districts()
    if not districts:
        print("No rows in DimDistrict. From the cropguard folder run: python main.py")
    for d in districts:
        print(f"Ingesting data for {d['name']}...")
        weather_data = fetch_weather(d["lat"], d["lon"])
        save_to_db(d["district_id"], weather_data)
    print("Ingestion complete!")
