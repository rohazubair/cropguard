from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd


def crop_envelopes(df: pd.DataFrame) -> dict[tuple[int, str], dict[str, float]]:
    if df.empty or "district_id" not in df.columns:
        return {}
    sub = df.dropna(subset=["district_id"]).copy()
    if sub.empty:
        return {}
    out: dict[tuple[int, str], dict[str, float]] = {}
    for (did, crop), g in sub.groupby(["district_id", "crop_name"], sort=False):
        out[(int(did), str(crop))] = {
            "t_p10": float(g["temperature"].quantile(0.1)),
            "t_p50": float(g["temperature"].quantile(0.5)),
            "t_p90": float(g["temperature"].quantile(0.9)),
            "h_p10": float(g["humidity"].quantile(0.1)),
            "h_p50": float(g["humidity"].quantile(0.5)),
            "h_p90": float(g["humidity"].quantile(0.9)),
        }
    return out


def score_crop_day(
    t_pred: float,
    h_pred: float,
    env: dict[str, float],
) -> tuple[float, str, dict[str, Any]]:
    drivers: dict[str, Any] = {}
    score = 0.0
    t90 = max(env["t_p90"], 1e-6)
    t10 = max(env["t_p10"], 1e-6)
    h90 = max(env["h_p90"], 1e-6)
    h10 = max(env["h_p10"], 1e-6)

    if t_pred > env["t_p90"]:
        x = float((t_pred - env["t_p90"]) / max(1.0, abs(t90)))
        drivers["heat_excess_vs_p90"] = round(x, 4)
        score += min(50.0, x * 12.0)
    if t_pred < env["t_p10"]:
        x = float((env["t_p10"] - t_pred) / max(1.0, abs(t10)))
        drivers["cold_excess_vs_p10"] = round(x, 4)
        score += min(50.0, x * 12.0)
    if h_pred < env["h_p10"]:
        x = float((env["h_p10"] - h_pred) / max(1.0, abs(h10)))
        drivers["dry_air_vs_p10"] = round(x, 4)
        score += min(35.0, x * 8.0)
    if h_pred > env["h_p90"]:
        x = float((h_pred - env["h_p90"]) / max(1.0, abs(h90)))
        drivers["humid_excess_vs_p90"] = round(x, 4)
        score += min(35.0, x * 8.0)

    score = float(min(100.0, score))
    if score < 12.0:
        tier = "none"
    elif score < 32.0:
        tier = "watch"
    elif score < 58.0:
        tier = "advisory"
    else:
        tier = "high"
    return score, tier, drivers


def valid_dates_from_anchor(anchor: pd.Timestamp, horizon: int) -> list[date]:
    a = pd.Timestamp(anchor).normalize()
    return [(a + timedelta(days=k + 1)).date() for k in range(horizon)]
