import sys

sys.dont_write_bytecode = True

from prefect import task, flow
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.ingest_weather import fetch_weather, get_districts, save_to_db

@task(retries=6, retry_delay_seconds=25)
def process_district_data(district):
    print(f"Prefect Task: Starting ingestion for {district['name']}...")
    data = fetch_weather(district["lat"], district["lon"])
    save_to_db(district["district_id"], data)
    return f"Success: {district['name']}"

@flow(name="CropGuard Weather Pipeline")
def weather_pipeline():
    print("Starting the CropGuard orchestrated pipeline...")
    districts = get_districts()
    if not districts:
        print("No rows in DimDistrict. From the cropguard folder run: python main.py")
        return []
    results = []
    for d in districts:
        status = process_district_data(d)
        results.append(status)
    print("Pipeline execution finished successfully!")
    return results

if __name__ == "__main__":
    weather_pipeline()