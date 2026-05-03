from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from psycopg2.extras import execute_batch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_a, **_kw):  # pragma: no cover
        return False

load_dotenv(PROJECT_ROOT / ".env")

from ingestion.ingest_weather import _configure_search_path, connect_db
from ingestion.silver_ge_validation import validate_silver_weather_readings_df

BRONZE_WEATHER_SELECT = """
    SELECT district_id, observed_ts, temperature, humidity_pct
    FROM bronze.weather_readings_fact
    """

SILVER_WEATHER_INSERT = """
    INSERT INTO silver.weather_readings_fact (
        district_id, observed_ts, temperature, humidity_pct
    ) VALUES (%s, %s, %s, %s)
    """


def _median_impute_by_district_then_global(
    series: pd.Series,
    district_id: pd.Series,
) -> tuple[pd.Series, int]:
    s = pd.to_numeric(series, errors="coerce")
    pre_null = s.isna()
    out = s.groupby(district_id, dropna=False).transform(lambda x: x.fillna(x.median()))
    gmed = out.median()
    if pd.isna(gmed):
        gmed = 0.0
    out = out.fillna(gmed)
    imputed = int((pre_null & out.notna()).sum())
    return out, imputed


def load_weather_readings_silver(*, truncate_before_load: bool = True) -> dict[str, Any]:
    rows: list[Any] = []
    out_rows: list[tuple[Any, ...]] = []
    dropped_missing_keys = 0
    imputed_temperature = 0
    imputed_humidity = 0
    ge_weather: dict[str, Any] = {}
    conn = connect_db()
    try:
        cur = conn.cursor()
        _configure_search_path(cur)
        cur.execute(BRONZE_WEATHER_SELECT)
        rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=["district_id", "observed_ts", "temperature", "humidity_pct"])
        if not df.empty:
            key_null = df["district_id"].isna() | df["observed_ts"].isna()
            dropped_missing_keys = int(key_null.sum())
            df = df.loc[~key_null].copy()

        if not df.empty:
            df["temperature"], imputed_temperature = _median_impute_by_district_then_global(
                df["temperature"], df["district_id"]
            )
            h_float, imputed_humidity = _median_impute_by_district_then_global(
                df["humidity_pct"], df["district_id"]
            )
            df["humidity_pct"] = np.clip(np.round(h_float), 0, 100).astype(int)
            df["temperature"] = df["temperature"].astype(float)
            df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["temperature", "humidity_pct"])
            ge_weather = validate_silver_weather_readings_df(df)

        out_rows = (
            list(
                zip(
                    df["district_id"].astype(int),
                    df["observed_ts"],
                    df["temperature"].astype(float),
                    df["humidity_pct"].astype(int),
                )
            )
            if not df.empty
            else []
        )

        if truncate_before_load:
            cur.execute("TRUNCATE TABLE silver.weather_readings_fact")
        if out_rows:
            execute_batch(cur, SILVER_WEATHER_INSERT, out_rows, page_size=2000)

        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "bronze_rows_read": len(rows),
        "rows_inserted": len(out_rows),
        "rows_dropped_missing_keys": dropped_missing_keys,
        "imputed_temperature_cells": imputed_temperature,
        "imputed_humidity_cells": imputed_humidity,
        "great_expectations": {"silver_weather_readings": ge_weather},
    }
