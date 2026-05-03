from __future__ import annotations
import os
import sys
from typing import Any
sys.dont_write_bytecode = True
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, get_run_logger, task
try:
    from prefect.tasks import exponential_backoff
except ImportError:
    try:
        from prefect import exponential_backoff
    except ImportError:
        exponential_backoff = None
from ingestion.apply_bronze_schema import apply_bronze_schema
from ingestion.ingest_weather import get_districts, save_to_db
from ingestion.load_crop_facts_bronze import load_crop_facts_bronze
from ingestion.load_crop_facts_silver import load_crop_facts_silver
from ingestion.load_weather_readings_silver import load_weather_readings_silver
from ingestion.seed_districts_geocode import seed_missing_districts
from prediction.forecast_pipeline import run_forecast_pipeline
_RETRIES = 3
if exponential_backoff is not None:
    _RETRY_DELAY = exponential_backoff(backoff_factor=8)
else:
    _RETRY_DELAY = [8, 32, 128]
_TASK = {'retries': _RETRIES, 'retry_delay_seconds': _RETRY_DELAY, 'retry_jitter_factor': 0.2}

@task(name='apply_bronze_schema', **_TASK)
def task_apply_bronze_schema() -> None:
    apply_bronze_schema()

@task(name='seed_districts_geocode', **_TASK)
def task_seed_districts_geocode() -> dict[str, Any]:
    return seed_missing_districts()

@task(name='load_crop_facts_csv', **_TASK)
def task_load_crop_facts_csv() -> dict[str, Any]:
    return load_crop_facts_bronze(skip_if_csv_unchanged=True)

@task(name='load_crop_facts_silver', **_TASK)
def task_load_crop_facts_silver() -> dict[str, Any]:
    return load_crop_facts_silver()

@task(name='load_weather_readings_silver', **_TASK)
def task_load_weather_readings_silver() -> dict[str, Any]:
    return load_weather_readings_silver()

@task(name='list_districts_dim', **_TASK)
def task_list_districts() -> list[dict]:
    return get_districts()

@task(name='forecast_pipeline', **_TASK)
def task_forecast_pipeline() -> dict[str, Any]:
    return run_forecast_pipeline()

@task(name='ingest_district_weather', **_TASK)
def task_ingest_district_weather(district: dict) -> str:
    name = district.get('name', '?')
    get_run_logger().info('Weather ingest for district: %s', name)
    summary = save_to_db(district)
    return f"Success: {name} (insert_attempts={summary.get('hours_insert_attempted', 0)}, cdc_skipped_hours={summary.get('hours_skipped_cdc', 0)})"

@flow(name='CropGuard Bronze Pipeline', retries=0)
def cropguard_bronze_pipeline() -> dict[str, Any]:
    log = get_run_logger()
    log.info('CropGuard: starting bronze pipeline (schema → seed → crop → silver → weather → forecast)')
    task_apply_bronze_schema()
    seed_summary = task_seed_districts_geocode()
    crop_summary = task_load_crop_facts_csv()
    silver_crop_summary = task_load_crop_facts_silver()
    districts = task_list_districts()
    if not districts:
        log.warning('No rows in bronze.districts_dim; skipping per-district weather ingest')
        silver_weather_summary = task_load_weather_readings_silver()
        forecast_summary = task_forecast_pipeline()
        log.info('Bronze pipeline finished (no districts; forecast_keys=%s)', list(forecast_summary.keys()) if isinstance(forecast_summary, dict) else type(forecast_summary).__name__)
        return {'districts': [], 'seed_districts': seed_summary, 'crop_facts': crop_summary, 'silver_crop_facts': silver_crop_summary, 'silver_weather_readings': silver_weather_summary, 'forecast': forecast_summary, 'weather_statuses': []}
    weather_statuses: list[str] = []
    for d in districts:
        weather_statuses.append(task_ingest_district_weather(d))
    silver_weather_summary = task_load_weather_readings_silver()
    forecast_summary = task_forecast_pipeline()
    log.info('Bronze pipeline finished (districts=%s, forecast_keys=%s)', len(districts), list(forecast_summary.keys()) if isinstance(forecast_summary, dict) else type(forecast_summary).__name__)
    return {'districts_count': len(districts), 'seed_districts': seed_summary, 'crop_facts': crop_summary, 'silver_crop_facts': silver_crop_summary, 'silver_weather_readings': silver_weather_summary, 'forecast': forecast_summary, 'weather_statuses': weather_statuses}

def weather_pipeline():
    return cropguard_bronze_pipeline()
if __name__ == '__main__':
    cropguard_bronze_pipeline()
