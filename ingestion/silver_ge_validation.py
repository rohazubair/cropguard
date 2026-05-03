from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import pandas as pd

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite


def _gx_validate(
    df: pd.DataFrame,
    *,
    suite_name: str,
    add_expectations: Callable[[ExpectationSuite], None],
) -> dict[str, Any]:
    if df.empty:
        return {"skipped": True, "success": True, "evaluated_expectations": 0}

    ctx = gx.get_context(mode="ephemeral")
    ds_name = f"silver_{suite_name}_{uuid.uuid4().hex[:10]}"
    data_source = ctx.data_sources.add_pandas(ds_name)
    asset = data_source.add_dataframe_asset(name="batch")
    batch_def = asset.add_batch_definition_whole_dataframe("whole")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = ExpectationSuite(name=f"{suite_name}_{uuid.uuid4().hex[:8]}")
    add_expectations(suite)
    result = batch.validate(suite)

    stats = getattr(result, "statistics", None) or {}
    out: dict[str, Any] = {
        "success": bool(result.success),
        "evaluated_expectations": stats.get("evaluated_expectations"),
        "successful_expectations": stats.get("successful_expectations"),
        "unsuccessful_expectations": stats.get("unsuccessful_expectations"),
        "success_percent": stats.get("success_percent"),
    }

    if not result.success:
        failures: list[str] = []
        for r in getattr(result, "results", []) or []:
            if getattr(r, "success", True):
                continue
            cfg = getattr(r, "expectation_config", None)
            typ = getattr(cfg, "type", None) or (cfg.get("type") if isinstance(cfg, dict) else None)
            msg = getattr(r, "exception_info", None)
            ex_msg = None
            if msg is not None:
                ex_msg = getattr(msg, "exception_message", None) or (
                    msg.get("exception_message") if isinstance(msg, dict) else None
                )
            failures.append(f"{typ}: {ex_msg or 'failed'}")
        out["failure_messages"] = failures[:20]
        raise ValueError(
            "Great Expectations validation failed for silver dataframe: "
            + "; ".join(failures[:8])
        )

    return out


def _add_crop_expectations(suite: ExpectationSuite) -> None:
    E = gx.expectations
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="district_id"))
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="temperature"))
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="humidity"))
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="crop_name"))
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="district"))
    suite.add_expectation(E.ExpectColumnValuesToBeBetween(column="district_id", min_value=1, max_value=50_000_000))
    suite.add_expectation(E.ExpectColumnValuesToBeBetween(column="temperature", min_value=-90.0, max_value=70.0))
    suite.add_expectation(E.ExpectColumnValuesToBeBetween(column="humidity", min_value=0.0, max_value=100.0))
    suite.add_expectation(E.ExpectColumnValueLengthsToBeBetween(column="crop_name", min_value=1, max_value=256))
    suite.add_expectation(E.ExpectColumnValueLengthsToBeBetween(column="district", min_value=1, max_value=128))


def _add_weather_expectations(suite: ExpectationSuite) -> None:
    E = gx.expectations
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="district_id"))
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="observed_ts"))
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="temperature"))
    suite.add_expectation(E.ExpectColumnValuesToNotBeNull(column="humidity_pct"))
    suite.add_expectation(E.ExpectColumnValuesToBeBetween(column="district_id", min_value=1, max_value=50_000_000))
    suite.add_expectation(E.ExpectColumnValuesToBeBetween(column="temperature", min_value=-90.0, max_value=70.0))
    suite.add_expectation(E.ExpectColumnValuesToBeBetween(column="humidity_pct", min_value=0, max_value=100))


def validate_silver_crop_facts_df(df: pd.DataFrame) -> dict[str, Any]:
    return _gx_validate(df, suite_name="crop_facts", add_expectations=_add_crop_expectations)


def validate_silver_weather_readings_df(df: pd.DataFrame) -> dict[str, Any]:
    return _gx_validate(df, suite_name="weather_readings", add_expectations=_add_weather_expectations)
