"""Helpers that translate deterministic temperature envelopes into categorical crop health."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable


@dataclass(frozen=True)
class DailyTemperatureStats:
    day: datetime.date
    min_c: float
    max_c: float
    mean_c: float


def hourly_forecast_to_daily(entries: Iterable[dict]) -> list[DailyTemperatureStats]:
    """Collapse hourly dictionaries into UTC calendar-day summaries."""

    buckets: dict[datetime.date, list[float]] = {}
    for row in entries:
        ts = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
        temps = buckets.setdefault(ts.date(), [])
        temps.append(float(row["temperature_c"]))

    daily: list[DailyTemperatureStats] = []
    for day in sorted(buckets.keys()):
        series = buckets[day]
        daily.append(
            DailyTemperatureStats(
                day=day,
                min_c=min(series),
                max_c=max(series),
                mean_c=sum(series) / len(series),
            )
        )

    return daily


def classify_day(
    stats: DailyTemperatureStats,
    *,
    district_band: tuple[float, float] | None,
    global_band: tuple[float, float],
    comfort_margin_c: float = 2.5,
    district_shock_margin_c: float = 8.5,
) -> str:
    """
    danger – Leaves the global percentile band or abruptly departs from district optima (shock thresholds).
    medium – Still survivable but outside the narrower district optimum band or near any envelope edge.
    healthy – Well within the district corridor (or, if absent, the global corridor) with margin to spare.
    """

    g_low, g_high = global_band
    if stats.min_c < g_low or stats.max_c > g_high:
        return "danger"

    if district_band:
        d_low, d_high = district_band

        shock_lower = stats.min_c < (d_low - district_shock_margin_c)
        shock_upper = stats.max_c > (d_high + district_shock_margin_c)
        if shock_lower or shock_upper:
            return "danger"

        outside_district_band = stats.min_c < d_low or stats.max_c > d_high
        borderline_district = stats.min_c < d_low + comfort_margin_c or stats.max_c > (
            d_high - comfort_margin_c
        )

        if outside_district_band or borderline_district:
            return "medium"

        return "healthy"

    borderline_global = stats.min_c < g_low + comfort_margin_c or stats.max_c > (
        g_high - comfort_margin_c
    )
    return "medium" if borderline_global else "healthy"
