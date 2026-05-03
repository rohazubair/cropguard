from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from prediction.forecast_constants import (
    ARTIFACT_SUBDIR,
    HORIZON_DAYS,
    LOOKBACK_DAYS,
    MIN_TRAIN_ROWS,
    MODEL_NAME_WEATHER_DAILY,
)
from prediction.forecast_daily_dataset import (
    build_supervised_daily,
    drift_reference_from_daily,
    hourly_to_daily,
)


def _make_pipeline(alpha: float) -> Pipeline:
    return Pipeline(
        steps=[
            (
                "prep",
                ColumnTransformer(
                    transformers=[
                        (
                            "cat",
                            OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
                            ["district_id"],
                        ),
                    ],
                    remainder="passthrough",
                ),
            ),
            (
                "model",
                MultiOutputRegressor(
                    HistGradientBoostingRegressor(
                        loss="quantile",
                        alpha=alpha,
                        learning_rate=0.07,
                        max_depth=12,
                        max_iter=280,
                        l2_regularization=0.2,
                        min_samples_leaf=10,
                        random_state=42,
                    )
                ),
            ),
        ]
    )


def _ordered_X(sup: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    return sup[feature_cols].copy()


def fit_weather_bundle(daily: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    sup, feature_cols = build_supervised_daily(daily)
    if sup.empty or len(sup) < MIN_TRAIN_ROWS:
        raise ValueError(f"Not enough supervised rows ({len(sup)}); need >= {MIN_TRAIN_ROWS}")

    y_t_cols = [f"y_temp_{k}" for k in range(HORIZON_DAYS)]
    y_h_cols = [f"y_hum_{k}" for k in range(HORIZON_DAYS)]
    X = _ordered_X(sup, feature_cols)
    y_temp = sup[y_t_cols].astype(float)
    y_hum = sup[y_h_cols].astype(float)

    X_tr, X_va, yt_tr, yt_va, yh_tr, yh_va = train_test_split(
        X, y_temp, y_hum, test_size=0.15, random_state=42, shuffle=True
    )

    quantiles = (0.1, 0.5, 0.9)
    pipes_temp: dict[float, Pipeline] = {}
    pipes_hum: dict[float, Pipeline] = {}
    val_mae_temp: dict[str, float] = {}
    val_mae_hum: dict[str, float] = {}

    for q in quantiles:
        pt = _make_pipeline(q)
        pt.fit(X_tr, yt_tr)
        pred = pt.predict(X_va)
        val_mae_temp[str(q)] = round(float(mean_absolute_error(yt_va.values, pred)), 4)
        pipes_temp[q] = pt

        ph = _make_pipeline(q)
        ph.fit(X_tr, yh_tr)
        predh = ph.predict(X_va)
        val_mae_hum[str(q)] = round(float(mean_absolute_error(yh_va.values, predh)), 4)
        pipes_hum[q] = ph

    for q in quantiles:
        pipes_temp[q].fit(X, y_temp)
        pipes_hum[q].fit(X, y_hum)

    drift_reference = drift_reference_from_daily(daily)
    train_end = pd.to_datetime(daily["date"]).max()
    meta = {
        "model_name": MODEL_NAME_WEATHER_DAILY,
        "n_supervised_rows": int(len(sup)),
        "n_daily_rows": int(len(daily)),
        "train_data_end_date": str(train_end.date()),
        "val_mae_temp_p50": val_mae_temp.get("0.5"),
        "val_mae_humidity_p50": val_mae_hum.get("0.5"),
        "feature_cols": feature_cols,
        "horizon_days": HORIZON_DAYS,
        "lookback_days": LOOKBACK_DAYS,
    }

    bundle: dict[str, Any] = {
        "pipelines_temp": pipes_temp,
        "pipelines_humidity": pipes_hum,
        "feature_cols": feature_cols,
        "drift_reference": drift_reference,
        "meta": meta,
    }
    metrics = {
        "val_mae_temp": val_mae_temp,
        "val_mae_humidity": val_mae_hum,
        "n_train_rows": int(len(X)),
    }
    return bundle, metrics


def save_bundle(project_root: Path, bundle: dict[str, Any], metrics: dict[str, Any]) -> tuple[str, str]:
    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    out_dir = project_root / "artifacts" / ARTIFACT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)
    versioned = out_dir / f"bundle_{version}.joblib"
    latest = out_dir / "bundle_latest.joblib"
    payload = {"bundle": bundle, "metrics": metrics, "version": version}
    joblib.dump(payload, versioned)
    joblib.dump(payload, latest)
    meta_path = out_dir / f"bundle_{version}_meta.json"
    meta_path.write_text(json.dumps({"version": version, "metrics": metrics, "meta": bundle["meta"]}, indent=2), encoding="utf-8")
    return version, str(versioned.resolve())


def train_from_silver_hourly(hourly: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    daily = hourly_to_daily(hourly)
    bundle, metrics = fit_weather_bundle(daily)
    root = Path(__file__).resolve().parents[1]
    version, uri = save_bundle(root, bundle, metrics)
    return bundle, metrics, version, uri
