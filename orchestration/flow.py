from prefect import task, flow
import sys
import os

# This allows Python to find your ingestion script from the orchestration folder
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.ingest_weather import fetch_weather, save_to_db, DISTRICTS

# The @task decorator tells Prefect to track this specific function.
# It includes the mandatory retry logic (retries 3 times, waits 10 seconds between tries).
@task(retries=3, retry_delay_seconds=10)
def process_district_data(district):
    print(f"Prefect Task: Starting ingestion for {district['name']}...")
    data = fetch_weather(district['lat'], district['lon'])
    save_to_db(district['name'], data)
    return f"Success: {district['name']}"

@flow(name="CropGuard Weather Pipeline")
def weather_pipeline():
    print("Starting the CropGuard orchestrated pipeline...")
    results = []
    for d in DISTRICTS:
        status = process_district_data(d)
        results.append(status)
    print("Pipeline execution finished successfully!")
    return results

if __name__ == "__main__":
    weather_pipeline()