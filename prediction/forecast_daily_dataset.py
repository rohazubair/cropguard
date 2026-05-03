from __future__ import annotations

from typing import Any

import pandas as pd

from prediction.forecast_constants import HORIZON_DAYS, LOOKBACK_DAYS


def hourly_to_daily(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly.empty:
        return pd.DataFrame(columns=["district_id", "date", "temp_max", "humidity_mean"])
    df = hourly.copy()
    df["observed_ts"] = pd.to_datetime(df["observed_ts"])
    df["date"] = df["observed_ts"].dt.normalize()
    g = (
        df.groupby(["district_id", "date"], as_index=False)
        .agg(temp_max=("temperature", "max"), humidity_mean=("humidity_pct", "mean"))
        .sort_values(["district_id", "date"])
        .reset_index(drop=True)
    )
    return g


def build_supervised_daily(daily: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    if daily.empty:
        return pd.DataFrame(), []
    tcols = [f"t_{i}" for i in range(LOOKBACK_DAYS)]
    hcols = [f"h_{i}" for i in range(LOOKBACK_DAYS)]
    y_t_cols = [f"y_temp_{k}" for k in range(HORIZON_DAYS)]
    y_h_cols = [f"y_hum_{k}" for k in range(HORIZON_DAYS)]
    feature_cols = ["district_id"] + tcols + hcols

    rows: list[dict[str, float | int]] = []
    for did, grp in daily.groupby("district_id", sort=False):
        grp = grp.sort_values("date").reset_index(drop=True)
        n = len(grp)
        need = LOOKBACK_DAYS + HORIZON_DAYS
        if n < need:
            continue
        for i in range(n - need + 1):
            window = grp.iloc[i : i + LOOKBACK_DAYS]
            future = grp.iloc[i + LOOKBACK_DAYS : i + need]
            row: dict[str, float | int] = {"district_id": int(did)}
            for j in range(LOOKBACK_DAYS):
                row[tcols[j]] = float(window["temp_max"].iloc[j])
                row[hcols[j]] = float(window["humidity_mean"].iloc[j])
            for k in range(HORIZON_DAYS):
                row[y_t_cols[k]] = float(future["temp_max"].iloc[k])
                row[y_h_cols[k]] = float(future["humidity_mean"].iloc[k])
            rows.append(row)

    out = pd.DataFrame(rows)
    return out, feature_cols


def drift_reference_from_daily(daily: pd.DataFrame) -> dict[str, Any]:
    per: dict[str, dict[str, float]] = {}
    for did, g in daily.groupby("district_id", sort=False):
        per[str(int(did))] = {
            "mean": float(g["temp_max"].mean()),
            "std": float(max(1e-6, float(g["temp_max"].std(ddof=0) or 0.0))),
        }
    return {"per_district_daily_max_temp": per}


def recent_daily_profile(daily: pd.DataFrame, *, last_n_days: int) -> dict[str, float]:
    if daily.empty:
        return {}
    dmax = daily["date"].max()
    start = dmax - pd.Timedelta(days=last_n_days - 1)
    sub = daily[daily["date"] >= start]
    out: dict[str, float] = {}
    for did, g in sub.groupby("district_id", sort=False):
        out[str(int(did))] = float(g["temp_max"].mean())
    return out
