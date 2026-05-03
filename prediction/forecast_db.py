"""Write ML registry, forecast runs, and point predictions to silver.ml_* tables (gold views read these)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from psycopg2.extensions import connection
from psycopg2.extras import Json, execute_batch

from ingestion.ingest_weather import _configure_search_path, connect_db


def db_conn() -> connection:
    return connect_db()


def get_active_model(cur, model_name: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT model_id, artifact_uri, version, feature_reference_json, metrics_json
        FROM silver.ml_model_registry
        WHERE model_name = %s AND is_active = TRUE
        ORDER BY model_id DESC
        LIMIT 1
        """,
        (model_name,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "model_id": int(row[0]),
        "artifact_uri": str(row[1]),
        "version": str(row[2]),
        "feature_reference_json": row[3] or {},
        "metrics_json": row[4] or {},
    }


def deactivate_models(cur, model_name: str) -> None:
    cur.execute(
        "UPDATE silver.ml_model_registry SET is_active = FALSE WHERE model_name = %s",
        (model_name,),
    )


def insert_model_registry(
    cur,
    *,
    model_name: str,
    version: str,
    artifact_uri: str,
    train_data_end_ts: datetime | None,
    metrics: dict[str, Any],
    feature_reference: dict[str, Any],
    set_active: bool,
) -> int:
    if set_active:
        deactivate_models(cur, model_name)
    cur.execute(
        """
        INSERT INTO silver.ml_model_registry (
            model_name, version, artifact_uri, train_data_end_ts,
            metrics_json, feature_reference_json, is_active
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING model_id
        """,
        (
            model_name,
            version,
            artifact_uri,
            train_data_end_ts,
            Json(metrics),
            Json(feature_reference),
            set_active,
        ),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("insert_model_registry failed")
    mid = int(row[0])
    if set_active:
        cur.execute(
            "UPDATE silver.ml_model_registry SET is_active = FALSE WHERE model_name = %s AND model_id <> %s",
            (model_name, mid),
        )
        cur.execute(
            "UPDATE silver.ml_model_registry SET is_active = TRUE WHERE model_id = %s",
            (mid,),
        )
    return mid


def insert_run(
    cur,
    *,
    model_id: int | None,
    input_weather_until: datetime,
    horizon_days: int,
    granularity: str,
) -> int:
    cur.execute(
        """
        INSERT INTO silver.ml_forecast_run (
            model_id, status, input_weather_until, horizon_days, granularity
        ) VALUES (%s, 'running', %s, %s, %s)
        RETURNING run_id
        """,
        (model_id, input_weather_until, horizon_days, granularity),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("insert_run failed")
    return int(row[0])


def finish_run(
    cur,
    run_id: int,
    *,
    status: str,
    drift_checked: bool,
    drift_retrain_triggered: bool,
    notes: str | None = None,
) -> None:
    cur.execute(
        """
        UPDATE silver.ml_forecast_run
        SET finished_at = now(), status = %s, drift_checked = %s,
            drift_retrain_triggered = %s, notes = COALESCE(%s, notes)
        WHERE run_id = %s
        """,
        (status, drift_checked, drift_retrain_triggered, notes, run_id),
    )


def publish_run(cur, run_id: int) -> None:
    cur.execute(
        """
        INSERT INTO silver.ml_forecast_run_published (key, run_id, published_at)
        VALUES ('default', %s, now())
        ON CONFLICT (key) DO UPDATE
        SET run_id = EXCLUDED.run_id, published_at = EXCLUDED.published_at
        """,
        (run_id,),
    )


def insert_weather_points(cur, run_id: int, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO silver.ml_forecast_weather_point (
            run_id, district_id, valid_date, horizon_day,
            temp_p10, temp_p50, temp_p90,
            humidity_p10, humidity_p50, humidity_p90
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id, district_id, valid_date) DO UPDATE SET
            horizon_day = EXCLUDED.horizon_day,
            temp_p10 = EXCLUDED.temp_p10,
            temp_p50 = EXCLUDED.temp_p50,
            temp_p90 = EXCLUDED.temp_p90,
            humidity_p10 = EXCLUDED.humidity_p10,
            humidity_p50 = EXCLUDED.humidity_p50,
            humidity_p90 = EXCLUDED.humidity_p90
        """
    execute_batch(cur, sql, rows, page_size=500)


def insert_crop_risk_points(cur, run_id: int, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO silver.ml_forecast_crop_risk_point (
            run_id, district_id, crop_name, valid_date, risk_score, risk_tier, drivers_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id, district_id, crop_name, valid_date) DO UPDATE SET
            risk_score = EXCLUDED.risk_score,
            risk_tier = EXCLUDED.risk_tier,
            drivers_json = EXCLUDED.drivers_json
        """
    batch_rows = [(r[0], r[1], r[2], r[3], r[4], r[5], Json(r[6])) for r in rows]
    execute_batch(cur, sql, batch_rows, page_size=500)


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
