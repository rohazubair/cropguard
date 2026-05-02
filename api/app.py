"""FastAPI entrypoint exposing deterministic crop stress scoring on top of the GBM hourly forecaster."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse, Response

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _district_id_to_str(value: Any) -> str:
    """JSON-safe id for browsers: BIGINT / Cockroach IDs exceed JS Number.MAX_SAFE_INTEGER."""
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


def _district_id_from_path(district_id: str) -> int:
    raw = district_id.strip()
    if not raw:
        raise HTTPException(status_code=422, detail="district_id must be a non-empty integer string.")
    try:
        value = int(raw, 10)
    except ValueError:
        raise HTTPException(status_code=422, detail="district_id must be a base-10 integer.") from None
    if raw not in {"0", str(value)}:
        raise HTTPException(
            status_code=422,
            detail="district_id must use canonical base-10 form (no leading zeros or sign prefixes).",
        )
    return value


if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from ingestion.ingest_weather import _configure_search_path, connect_db  # noqa: E402

from prediction.forecast_temperature import iterative_forecast, load_bundle  # noqa: E402

from api.health_engine import classify_day, hourly_forecast_to_daily  # noqa: E402

ARTIFACT_DEFAULT = PROJECT_ROOT / "artifacts" / "temperature_model.joblib"
METRICS_DEFAULT = PROJECT_ROOT / "artifacts" / "temperature_model_metrics.json"

_APP_STATE: dict[str, Any] = {"bundle": None}


def _get_bundle() -> dict[str, Any]:
    if _APP_STATE["bundle"] is None:
        if not ARTIFACT_DEFAULT.exists():
            raise HTTPException(
                status_code=503,
                detail="Trained model artifact missing. Run `python -m prediction.train_temperature` from the cropguard directory.",
            )
        _APP_STATE["bundle"] = load_bundle(str(ARTIFACT_DEFAULT))
    return _APP_STATE["bundle"]


def _connect():
    conn = connect_db()
    cur = conn.cursor()
    _configure_search_path(cur)
    return conn, cur


app = FastAPI(title="CropGuard Advisor", version="0.3.0")


@app.get("/")
def root() -> RedirectResponse:
    """No HTML shell — send browsers straight to Swagger."""
    return RedirectResponse(url="/docs")


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/districts")
def list_districts() -> dict[str, list[dict[str, Any]]]:
    conn, cur = _connect()
    try:
        cur.execute(
            """
            SELECT district_id, name, province, lat::float8, lon::float8
            FROM DimDistrict
            ORDER BY name
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    payload = []
    for row in rows:
        payload.append(
            {
                "district_id": _district_id_to_str(row[0]),
                "name": row[1],
                "province": row[2],
                "lat": row[3],
                "lon": row[4],
            }
        )

    return {"districts": payload}


@app.get("/model/temperature-metrics")
def model_metrics() -> dict[str, Any]:
    if not METRICS_DEFAULT.exists():
        raise HTTPException(
            status_code=503,
            detail="Run `python -m prediction.train_temperature` once to persist metrics JSON.",
        )
    return json.loads(METRICS_DEFAULT.read_text(encoding="utf-8"))


@app.get("/districts/{district_id}/forecast-health")
def forecast_crop_health(
    district_id: str,
    forecast_days: int = Query(default=7, ge=1, le=14),
) -> dict[str, Any]:
    district_id_val = _district_id_from_path(district_id)

    bundle = _get_bundle()

    conn, cur = _connect()
    try:
        cur.execute(
            """
            SELECT district_id, name, lat::float8, lon::float8
            FROM DimDistrict
            WHERE district_id = %s
            """,
            (district_id_val,),
        )
        district_row = cur.fetchone()
        if not district_row:
            raise HTTPException(status_code=404, detail="District not configured in DimDistrict.")

        cur.execute(
            """
            SELECT
                dc.crop_id,
                dc.name,
                rg.temp_min_c AS global_low,
                rg.temp_max_c AS global_high,
                fd.temp_min_c AS district_low,
                fd.temp_max_c AS district_high
            FROM DimCrop dc
            INNER JOIN FactCropTemperatureRange rg ON rg.crop_id = dc.crop_id
            LEFT JOIN FactCropDistrictHealthyTemp fd
                ON fd.crop_id = dc.crop_id AND fd.district_id = %s
            ORDER BY dc.name
            """,
            (district_id_val,),
        )
        crop_rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not crop_rows:
        raise HTTPException(
            status_code=409,
            detail="Crop tables are empty. Run `python -m ingestion.load_crop_tables` after seeding the schema.",
        )

    district_meta = {
        "district_id": _district_id_to_str(district_row[0]),
        "name": district_row[1],
        "lat": district_row[2],
        "lon": district_row[3],
    }

    horizon_hours = int(forecast_days * 24)

    hourly_forecast, issues = iterative_forecast(
        district_id=district_id_val,
        lat=district_meta["lat"],
        lon=district_meta["lon"],
        bundle=bundle,
        horizon_hours=horizon_hours,
    )

    daily_stats = hourly_forecast_to_daily(hourly_forecast)
    if not daily_stats:
        raise HTTPException(
            status_code=502,
            detail="Hourly forecast did not produce any daily summaries (Open-Meteo returned an empty window).",
        )

    crop_payload: list[dict[str, Any]] = []
    for row in crop_rows:
        _, crop_name, g_low, g_high, d_low, d_high = row
        district_band = None
        if d_low is not None and d_high is not None:
            district_band = (float(d_low), float(d_high))
        global_band = (float(g_low), float(g_high))

        day_scores: list[dict[str, Any]] = []
        for stats in daily_stats:
            status = classify_day(
                stats,
                district_band=district_band,
                global_band=global_band,
            )
            day_scores.append(
                {
                    "date": stats.day.isoformat(),
                    "predicted_min_c": round(stats.min_c, 2),
                    "predicted_max_c": round(stats.max_c, 2),
                    "predicted_mean_c": round(stats.mean_c, 2),
                    "status": status,
                }
            )

        worst = max(
            day_scores,
            key=lambda item: {"healthy": 0, "medium": 1, "danger": 2}[item["status"]],
        )

        crop_payload.append(
            {
                "crop": crop_name,
                "global_optimal_range_c": {"min": global_band[0], "max": global_band[1]},
                "district_optimal_range_c": (
                    {"min": district_band[0], "max": district_band[1]} if district_band else None
                ),
                "daily": day_scores,
                "worst_case": worst,
            }
        )

    return {
        "district": district_meta,
        "model": {
            "name": "sklearn.hist_gradient_boosting.next_hour_recursive",
            "artifact": str(ARTIFACT_DEFAULT),
            "training_metrics": bundle.get("metrics"),
        },
        "forecast": {
            "days": forecast_days,
            "hourly_points": len(hourly_forecast),
            "validation_notes": issues,
        },
        "crops": crop_payload,
    }
