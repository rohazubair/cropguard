"""Train a pooled Histogram Gradient Boosted model for hourly temperature."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import requests
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from ingestion.ingest_weather import get_districts
from ingestion.weather_validation import hourly_json_to_frame, validate_and_clean_hourly

from prediction.features import engineer_hourly_series, supervised_frame


def fetch_training_payload(lat: float, lon: float, *, past_days: int = 60, forecast_days: int = 16) -> dict[str, Any]:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,relative_humidity_2m,soil_moisture_0_to_7cm",
        "past_days": past_days,
        "forecast_days": forecast_days,
        "timezone": "auto",
    }
    r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def assemble_engineered_only(*, past_days: int = 60) -> tuple[pd.DataFrame, dict[str, Any]]:
    districts = get_districts()
    if not districts:
        raise RuntimeError("No DimDistrict rows found; populate database first.")

    frames: list[pd.DataFrame] = []
    meta: dict[str, Any] = {"district_reports": {}, "total_rows_clean": 0}

    for d in districts:
        payload = fetch_training_payload(float(d["lat"]), float(d["lon"]), past_days=past_days)
        raw_df = hourly_json_to_frame(payload)
        clean_df, report = validate_and_clean_hourly(raw_df)
        meta["district_reports"][d["name"]] = {"rows": len(clean_df), "issues": report.issues}
        if clean_df.empty:
            meta["district_reports"][d["name"]]["skipped"] = True
            continue

        cd = clean_df.copy()
        cd["district_id"] = int(d["district_id"])
        meta["total_rows_clean"] += len(cd)
        meta["district_reports"][d["name"]]["skipped"] = False
        frames.append(
            cd[
                [
                    "ts",
                    "district_id",
                    "temperature_c",
                    "precipitation_mm",
                    "humidity_pct",
                    "soil_moisture",
                ]
            ]
        )

    if not frames:
        raise RuntimeError("Training aborted: empty weather snapshots for all districts.")

    combined = pd.concat(frames, ignore_index=True)

    engineered = engineer_hourly_series(combined, validate_cols=True)

    meta.update(
        {
            "columns_engineered": list(engineered.columns),
            "rows_engineered": len(engineered),
            "districts_loaded": sum(1 for v in meta["district_reports"].values() if not v.get("skipped")),
        }
    )

    return engineered, meta


def finalize_column_order(X: pd.DataFrame) -> list[str]:
    return ["district_id"] + sorted([c for c in X.columns if c != "district_id"])


def train_and_persist(output_dir: Path | str | None = None, *, past_days: int = 60) -> dict[str, Any]:
    out_dir = Path(output_dir or Path(__file__).resolve().parents[1] / "artifacts")
    out_dir.mkdir(parents=True, exist_ok=True)

    engineered, meta = assemble_engineered_only(past_days=past_days)
    X_all, y_all, timestamps = supervised_frame(engineered)

    meta["n_supervised_samples"] = len(X_all)
    meta["past_days_used"] = past_days

    if X_all.empty or len(X_all) < 1200:
        raise RuntimeError(
            "Not enough supervised samples (<1200). Increase `--past-days` or ensure DimDistrict is populated "
            "with geographic coordinates wired to weather-capable climates."
        )

    cutoff = timestamps.max() - pd.Timedelta(hours=120)
    train_mask = timestamps <= cutoff

    X_tr = X_all.loc[train_mask].reset_index(drop=True)
    y_tr = y_all.loc[train_mask].reset_index(drop=True)
    X_va = X_all.loc[~train_mask].reset_index(drop=True)
    y_va = y_all.loc[~train_mask].reset_index(drop=True)

    if len(X_va) < 200:
        X_tr, X_va, y_tr, y_va = train_test_split_manual(X_all, y_all, test_size=0.15)

    feature_order = finalize_column_order(X_all)

    pipe = Pipeline(
        steps=[
            (
                "prep",
                ColumnTransformer(
                    transformers=[
                        (
                            "cat",
                            OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
                            ["district_id"],
                        )
                    ],
                    remainder="passthrough",
                ),
            ),
            (
                "model",
                HistGradientBoostingRegressor(
                    learning_rate=0.05,
                    max_depth=11,
                    max_iter=450,
                    l2_regularization=0.35,
                    min_samples_leaf=25,
                    random_state=42,
                ),
            ),
        ]
    )

    pipe.fit(X_tr[feature_order], y_tr)
    preds = pipe.predict(X_va[feature_order])
    metrics = {
        "mae_holdout_hours": round(float(mean_absolute_error(y_va, preds)), 4),
        "rmse_holdout_hours": round(float(np.sqrt(mean_squared_error(y_va, preds))), 4),
        "train_rows": int(len(X_tr)),
        "validation_rows": int(len(X_va)),
    }

    pipe.fit(X_all[feature_order], y_all)

    bundle = {
        "pipeline": pipe,
        "feature_column_order": feature_order,
        "metrics": metrics,
        **meta,
    }

    artifact_path = out_dir / "temperature_model.joblib"
    metrics_path = out_dir / "temperature_model_metrics.json"
    joblib.dump(bundle, artifact_path)
    metrics_path.write_text(
        json.dumps({"metrics": metrics, "district_reports": meta["district_reports"]}, indent=2),
        encoding="utf-8",
    )

    return {"artifact": str(artifact_path), **metrics}


def train_test_split_manual(
    X: pd.DataFrame, y: pd.Series, *, test_size: float = 0.15
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    n = len(X)
    cut = int(n * (1 - test_size))
    return X.iloc[:cut], X.iloc[cut:], y.iloc[:cut], y.iloc[cut:]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--past-days", type=int, default=60)
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()
    outp = Path(args.out).resolve() if args.out else None
    summary = train_and_persist(output_dir=outp, past_days=args.past_days)
    print(json.dumps(summary, indent=2))
