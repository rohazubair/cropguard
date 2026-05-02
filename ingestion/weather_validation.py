"""
Validation and lightweight cleaning for Open-Meteo style hourly payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class WeatherValidationReport:
    n_rows_raw: int
    n_rows_after_clean: int
    issues: list[str]


def hourly_json_to_frame(payload: dict[str, Any]) -> pd.DataFrame:
    """Flatten Open-Meteo hourly block into a time-indexed DataFrame."""
    h = payload.get("hourly") or {}
    times = h.get("time") or []
    if not times:
        return pd.DataFrame()

    cols = {
        "ts": pd.to_datetime(times, utc=True),
        "temperature_c": h.get("temperature_2m"),
        "precipitation_mm": h.get("precipitation"),
        "humidity_pct": h.get("relative_humidity_2m"),
        "soil_moisture": h.get("soil_moisture_0_to_7cm"),
    }

    lengths = [len(times)]
    values = cols.values()
    for v in values:
        if v is None:
            continue
        lengths.append(len(v))
    target_len = min(lengths) if lengths else 0

    data = {}
    data["ts"] = cols["ts"][:target_len]
    data["temperature_c"] = cols["temperature_c"][:target_len] if cols["temperature_c"] is not None else None
    data["precipitation_mm"] = cols["precipitation_mm"][:target_len] if cols["precipitation_mm"] is not None else None
    data["humidity_pct"] = cols["humidity_pct"][:target_len] if cols["humidity_pct"] is not None else None
    data["soil_moisture"] = cols["soil_moisture"][:target_len] if cols["soil_moisture"] is not None else None

    df = pd.DataFrame(data)

    geo_lat = payload.get("latitude")
    geo_lon = payload.get("longitude")
    if geo_lat is not None:
        df["latitude"] = float(geo_lat)
    if geo_lon is not None:
        df["longitude"] = float(geo_lon)

    df = df.sort_values("ts").reset_index(drop=True)
    return df


def validate_and_clean_hourly(df: pd.DataFrame) -> tuple[pd.DataFrame, WeatherValidationReport]:
    """
    - Enforce chronological order / drop duplicates
    - Numeric coercion & physical bounds
    - Optional outlier damping on temperature (median filter window 3 — keeps signal)
    - Forward/back fill small gaps within series (max 3 consecutive NA on aux fields)
    """
    issues: list[str] = []
    n_raw = len(df)

    if df.empty:
        return df, WeatherValidationReport(n_raw, 0, ["empty_input"])

    out = df.copy()
    required = {"ts", "temperature_c"}
    missing_cols = required - set(out.columns)
    if missing_cols:
        issues.append(f"missing_columns:{sorted(missing_cols)}")
        return pd.DataFrame(), WeatherValidationReport(n_raw, 0, issues)

    out = out.drop_duplicates(subset=["ts"], keep="last").sort_values("ts").reset_index(drop=True)

    for col in ("temperature_c", "precipitation_mm", "humidity_pct", "soil_moisture"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["temperature_c"])
    if out.empty:
        issues.append("no_valid_temperature_rows")
        return out, WeatherValidationReport(n_raw, 0, issues)

    out = out[(out["temperature_c"] >= -60) & (out["temperature_c"] <= 60)]
    if "humidity_pct" in out.columns:
        out.loc[out["humidity_pct"] < 0, "humidity_pct"] = np.nan
        out.loc[out["humidity_pct"] > 100, "humidity_pct"] = np.nan
    if "precipitation_mm" in out.columns:
        out.loc[out["precipitation_mm"] < 0, "precipitation_mm"] = 0.0
    if "soil_moisture" in out.columns:
        out.loc[(out["soil_moisture"] < 0) | (out["soil_moisture"] > 1), "soil_moisture"] = np.nan

    if len(out) >= 3:
        med = out["temperature_c"].rolling(window=3, center=True, min_periods=1).median()
        resid = (out["temperature_c"] - med).abs()
        spike = resid > 12
        if spike.any():
            issues.append(f"temperature_spike_replaced:{int(spike.sum())}")
            out.loc[spike, "temperature_c"] = med[spike]

    for aux in ("precipitation_mm", "humidity_pct", "soil_moisture"):
        if aux in out.columns:
            out[aux] = out[aux].interpolate(limit=3, limit_direction="both")
            if aux == "precipitation_mm":
                out[aux] = out[aux].fillna(0.0)
            else:
                out[aux] = out[aux].ffill().bfill()

    out = out.reset_index(drop=True)
    return out, WeatherValidationReport(n_raw, len(out), issues)
