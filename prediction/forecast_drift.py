from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from prediction.forecast_constants import DRIFT_Z_THRESHOLD
from prediction.forecast_daily_dataset import recent_daily_profile


def should_retrain_from_drift(
    daily: pd.DataFrame,
    drift_reference: dict[str, Any] | None,
    *,
    recent_days: int = 7,
    threshold: float = DRIFT_Z_THRESHOLD,
) -> bool:
    if daily.empty or not drift_reference:
        return False
    ref = drift_reference.get("per_district_daily_max_temp")
    if not isinstance(ref, dict) or not ref:
        return False
    recent = recent_daily_profile(daily, last_n_days=recent_days)
    if not recent:
        return False
    zs: list[float] = []
    for did, mu_r in recent.items():
        cell = ref.get(did)
        if not cell:
            continue
        mu0 = float(cell.get("mean", 0.0))
        sig = float(max(1e-6, float(cell.get("std", 1e-6))))
        zs.append(abs(float(mu_r) - mu0) / sig)
    if not zs:
        return False
    return float(np.mean(zs)) > threshold
