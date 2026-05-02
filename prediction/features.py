"""Feature engineering for hourly temperature forecasting (district-stratified series)."""

from __future__ import annotations

import numpy as np
import pandas as pd


MIN_HISTORY = 200  # Hours with full feature coverage after engineering


def _add_time_cyclicals(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    utc = pd.to_datetime(out["ts"], utc=True)
    hour = utc.dt.hour + utc.dt.minute / 60.0
    doy = utc.dt.dayofyear
    out["hour_sin"] = np.sin(2 * np.pi * hour / 24.0)
    out["hour_cos"] = np.cos(2 * np.pi * hour / 24.0)
    out["doy_sin"] = np.sin(2 * np.pi * doy / 366.0)
    out["doy_cos"] = np.cos(2 * np.pi * doy / 366.0)
    return out


def engineer_hourly_series(df: pd.DataFrame, *, validate_cols: bool = True) -> pd.DataFrame:
    """
    Builds supervised rows for forecasting next-step temperature inside each district.
    Expect columns: ts, district_id, temperature_c, precipitation_mm, humidity_pct, soil_moisture
    """
    if df.empty:
        return pd.DataFrame()

    required = {"ts", "district_id", "temperature_c"}
    if validate_cols:
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"engineer_hourly_series missing columns: {sorted(missing)}")

    ordered = df.sort_values(["district_id", "ts"]).reset_index(drop=True)

    def enrich(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        g = g.sort_values("ts")

        tc = g["temperature_c"]

        def safe_shift(s: pd.Series, h: int) -> pd.Series:
            return tc.shift(h)

        g["lag_1"] = safe_shift(tc, 1)
        g["lag_24"] = safe_shift(tc, 24)
        g["lag_168"] = safe_shift(tc, 168)
        g["delta_1"] = tc.diff(1)

        g["roll_mean_24"] = tc.rolling(24, min_periods=12).mean()
        g["roll_std_24"] = tc.rolling(24, min_periods=12).std()
        g["roll_min_48"] = tc.rolling(48, min_periods=24).min()
        g["roll_max_48"] = tc.rolling(48, min_periods=24).max()

        if "precipitation_mm" in g.columns:
            g["precip_sum_24"] = g["precipitation_mm"].rolling(24, min_periods=12).sum()
        else:
            g["precip_sum_24"] = np.nan

        g["forecast_target"] = tc.shift(-1)
        return g

    parts = []
    for _, grp in ordered.groupby("district_id", sort=False):
        parts.append(enrich(grp))
    out = pd.concat(parts, ignore_index=True)
    out = _add_time_cyclicals(out)

    cols = (
        [
            "ts",
            "district_id",
            "temperature_c",
            "forecast_target",
            "lag_1",
            "lag_24",
            "lag_168",
            "delta_1",
            "roll_mean_24",
            "roll_std_24",
            "roll_min_48",
            "roll_max_48",
            "precip_sum_24",
            "hour_sin",
            "hour_cos",
            "doy_sin",
            "doy_cos",
        ]
        + [c for c in ("precipitation_mm", "humidity_pct", "soil_moisture") if c in df.columns]
    )

    cols = [c for c in cols if c in out.columns]
    return out[cols]


def supervised_frame(engineered: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Drops rows lacking full feature history; returns aligned X (features-only), y, and timestamps.
    """
    if engineered.empty:
        empty = pd.DataFrame()
        return empty, pd.Series(dtype=float), pd.Series(dtype="datetime64[ns, UTC]")

    numeric_required = ["lag_1", "lag_24", "lag_168", "roll_mean_24", "roll_std_24", "precip_sum_24"]
    mask = pd.Series(True, index=engineered.index)
    for col in numeric_required:
        mask &= engineered[col].notna()

    sub = engineered.loc[mask].copy()
    mask_target = sub["forecast_target"].notna()
    sub = sub.loc[mask_target]
    y = sub["forecast_target"]

    numeric_features = [
        "lag_1",
        "lag_24",
        "lag_168",
        "delta_1",
        "roll_mean_24",
        "roll_std_24",
        "roll_min_48",
        "roll_max_48",
        "precip_sum_24",
        "hour_sin",
        "hour_cos",
        "doy_sin",
        "doy_cos",
    ]

    extras = ["precipitation_mm", "humidity_pct", "soil_moisture"]

    cols_x = numeric_features + [c for c in extras if c in sub.columns] + ["district_id"]
    cols_x = [c for c in cols_x if c in sub.columns]
    X = sub[cols_x].copy()
    ts = pd.to_datetime(sub["ts"], utc=True)

    return X.reset_index(drop=True), y.reset_index(drop=True), ts.reset_index(drop=True)


def feature_names_numeric(X: pd.DataFrame) -> list[str]:
    return [c for c in X.columns if c != "district_id"]
