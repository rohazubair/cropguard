"""Recursive multi-step temperature forecasts using pooled HistGradientBoosting model."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import joblib
import pandas as pd

from ingestion.weather_validation import hourly_json_to_frame, validate_and_clean_hourly
from prediction.features import engineer_hourly_series
from prediction.train_temperature import fetch_training_payload


def load_bundle(model_path: str | Path) -> dict[str, Any]:
    bundle = joblib.load(model_path)
    if "pipeline" not in bundle:
        raise ValueError("Model bundle missing sklearn pipeline.")
    return bundle


def build_last_inference_matrix(work: pd.DataFrame, bundle: dict[str, Any]) -> pd.DataFrame:
    engineered = engineer_hourly_series(work, validate_cols=True)
    tail_row = engineered.iloc[-1]

    pipe_order: list[str] = bundle["feature_column_order"]

    try:
        values = [tail_row[col] for col in pipe_order]
    except KeyError as exc:  # pragma: no cover
        missing = sorted(set(pipe_order).difference(set(tail_row.index)))
        raise ValueError(f"Inference row missing engineered columns: {missing}") from exc

    frame = pd.DataFrame([values], columns=pipe_order)
    nan_cols = frame.columns[np.array(frame.iloc[0].isna())].tolist()
    if nan_cols:
        raise ValueError(f"NaNs detected before inference ({nan_cols}); extend warmup history.")

    frame["district_id"] = pd.to_numeric(frame["district_id"], errors="raise").astype(int)
    frame = frame[pipe_order]
    return frame.reset_index(drop=True)


def _safe_tail(series: pd.Series, key: str, default: float) -> float:
    value = series.get(key)
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return default
    return float(value)


def iterative_forecast(
    *,
    district_id: int,
    lat: float,
    lon: float,
    bundle: dict[str, Any],
    horizon_hours: int,
    warmup_past_days: int = 40,
    forecast_anchor_days: int = 14,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Recursive horizon forecast (hourly resolution) leveraging the pooled Histogram GBM.
    """

    issues: list[str] = []

    payload = fetch_training_payload(
        float(lat),
        float(lon),
        past_days=warmup_past_days,
        forecast_days=max(forecast_anchor_days, int(horizon_hours // 24) + 7),
    )

    raw = hourly_json_to_frame(payload)
    clean, report = validate_and_clean_hourly(raw)
    issues.extend(report.issues)

    if len(clean) < 260:
        msg = (
            "Insufficient cleaned hourly history for inference — "
            f"need roughly 260 observations, observed {len(clean)} rows."
        )
        issues.append(msg)
        raise RuntimeError(msg)

    work = clean.copy()
    work["district_id"] = int(district_id)

    precip_tail = _safe_tail(work.iloc[-1], "precipitation_mm", 0.0)
    humid_tail = _safe_tail(work.iloc[-1], "humidity_pct", 55.0)
    soil_tail = _safe_tail(work.iloc[-1], "soil_moisture", 0.1)

    pipe_order: list[str] = bundle["feature_column_order"]
    model = bundle["pipeline"]

    forecasts: list[dict[str, Any]] = []

    for _ in range(int(horizon_hours)):
        X_infer = build_last_inference_matrix(work, bundle)[pipe_order]
        predicted = float(model.predict(X_infer)[0])

        last_ts = pd.to_datetime(work.iloc[-1]["ts"], utc=True)
        next_ts = last_ts + pd.Timedelta(hours=1)

        work = pd.concat(
            [
                work,
                pd.DataFrame(
                    [
                        {
                            "ts": next_ts,
                            "district_id": int(district_id),
                            "temperature_c": predicted,
                            "precipitation_mm": precip_tail,
                            "humidity_pct": humid_tail,
                            "soil_moisture": soil_tail,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

        forecasts.append(
            {
                "timestamp": next_ts.isoformat(),
                "temperature_c": round(predicted, 3),
            }
        )

    return forecasts, issues
