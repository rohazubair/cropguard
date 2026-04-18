import requests
import psycopg2
from datetime import datetime

DISTRICTS = [
    {"name": "Lahore", "lat": 31.52, "lon": 74.35},
    {"name": "Sahiwal", "lat": 30.66, "lon": 73.11}
]

def fetch_weather(lat, lon):
    """Fetch hourly weather variables defined in slides"""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,relative_humidity_2m,soil_moisture_0_to_7cm",
        "timezone": "auto",
        "forecast_days": 1
    }
    response = requests.get(url, params=params, timeout=10)
    return response.json()

def save_to_db(district_name, data):
    """Implements idempotent upsert logic"""
    conn = psycopg2.connect(
        host="127.0.0.1",
        port="5433", 
        database="cropguard",
        user="admin",
        password="password123"
    )
    cur = conn.cursor()
    
    cur.execute("SELECT district_id FROM DimDistrict WHERE name = %s", (district_name,))
    district_id = cur.fetchone()[0]

    hourly = data['hourly']
    for i in range(len(hourly['time'])):
        cur.execute("""
            INSERT INTO FactWeatherReadings (district_id, timestamp, temp_min, precipitation_mm, humidity_pct, soil_moisture)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (district_id, timestamp) DO NOTHING;
        """, (
            district_id,
            hourly['time'][i],
            hourly['temperature_2m'][i],
            hourly['precipitation'][i],
            hourly['relative_humidity_2m'][i],
            hourly['soil_moisture_0_to_7cm'][i]
        ))
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    for d in DISTRICTS:
        print(f"Ingesting data for {d['name']}...")
        weather_data = fetch_weather(d['lat'], d['lon'])
        save_to_db(d['name'], weather_data)
    print("Ingestion Complete!")