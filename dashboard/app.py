"""
CropGuard forecast dashboards — data via FastAPI (`/dashboard/*`), not direct DB access.
Run API: `uvicorn api.app:app --reload` from repo root.
Run UI: `streamlit run dashboard/app.py`
Set `CROPGUARD_API_BASE` if the API is not at http://127.0.0.1:8000
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def _api_base() -> str:
    base = os.environ.get("CROPGUARD_API_BASE", "http://127.0.0.1:8000").strip().rstrip("/")
    if not base:
        st.error("CROPGUARD_API_BASE is empty. Set it in `.env` or unset to use the default.")
        st.stop()
    return base


def _get_json(api_base: str, path: str) -> dict[str, Any]:
    url = f"{api_base}{path}"
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
    except requests.RequestException as exc:
        st.error(
            f"Could not reach CropGuard API at `{url}`. "
            f"Start the server with `uvicorn api.app:app --reload` from the repo root. ({exc})"
        )
        st.stop()
    return r.json()


@st.cache_data(ttl=60)
def load_published_run(api_base: str) -> pd.DataFrame:
    data = _get_json(api_base, "/dashboard/published-run")
    rows = data.get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return pd.DataFrame()
    return pd.DataFrame(rows)


@st.cache_data(ttl=60)
def load_forecast_weather(api_base: str) -> pd.DataFrame:
    data = _get_json(api_base, "/dashboard/forecast-weather")
    rows = data.get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return pd.DataFrame()
    return pd.DataFrame(rows)


@st.cache_data(ttl=60)
def load_forecast_crop_risk(api_base: str) -> pd.DataFrame:
    data = _get_json(api_base, "/dashboard/forecast-crop-risk")
    rows = data.get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _fmt_ts(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return str(val)[:19]


def main() -> None:
    st.set_page_config(page_title="CropGuard Forecast", layout="wide", initial_sidebar_state="expanded")
    st.title("CropGuard forecast dashboards")
    api_base = _api_base()
    st.caption(f"Data source: **FastAPI** `{api_base}/dashboard/*` (gold views on the server).")

    with st.sidebar:
        st.header("About")
        st.markdown(
            "This app loads the **published** ML forecast run through the CropGuard API. "
            "Run **`uvicorn api.app:app`** from the repo root, then refresh here. "
            "Populate data with `python main.py` or `python main.py --forecast-only`."
        )
        st.markdown(f"**API base:** `{api_base}`")
        if st.button("Refresh data"):
            st.cache_data.clear()
            st.rerun()

    run_df = load_published_run(api_base)
    weather_df = load_forecast_weather(api_base)
    risk_df = load_forecast_crop_risk(api_base)

    if run_df.empty and weather_df.empty and risk_df.empty:
        st.warning(
            "No published forecast data yet. The gold views are empty until a forecast run completes successfully."
        )
        st.stop()

    tab_run, tab_weather, tab_risk = st.tabs(["Published run", "Weather forecast", "Crop risk"])

    with tab_run:
        st.subheader("Published forecast run")
        if run_df.empty:
            st.info("No row in `gold.v_dashboard_published_forecast_run` (nothing published yet).")
        else:
            r = run_df.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Run ID", str(int(r["run_id"])) if pd.notna(r.get("run_id")) else "—")
            c2.metric("Status", str(r.get("status", "—")))
            c3.metric("Model", str(r.get("model_name", "—")))
            c4.metric("Version", str(r.get("model_version", "—")))
            st.write("**Horizon / granularity**", f"{r.get('horizon_days', '—')} days · {r.get('granularity', '—')}")
            st.write("**Input weather through**", _fmt_ts(r.get("input_weather_until")))
            st.write("**Run window**", f"{_fmt_ts(r.get('started_at'))} → {_fmt_ts(r.get('finished_at'))}")
            st.write("**Published at**", _fmt_ts(r.get("published_at")))
            st.write("**Drift**", f"checked={r.get('drift_checked')} · retrain_triggered={r.get('drift_retrain_triggered')}")
            st.write("**Artifact**", str(r.get("artifact_uri", "—")))

    with tab_weather:
        st.subheader("District weather (published run)")
        if weather_df.empty:
            st.info("`gold.v_dashboard_forecast_weather` has no rows.")
        else:
            districts = sorted(weather_df["district_name"].dropna().unique().tolist())
            pick = st.multiselect("Districts", districts, default=districts[: min(5, len(districts))])
            sub = weather_df[weather_df["district_name"].isin(pick)] if pick else weather_df
            for dname, g in sub.groupby("district_name", sort=True):
                st.markdown(f"#### {dname}")
                g2 = g.sort_values("valid_date").copy()
                g2["valid_date"] = pd.to_datetime(g2["valid_date"])
                tchart = g2[["valid_date", "temp_p10", "temp_p50", "temp_p90"]]
                st.caption("Temperature (°C) — P10 / P50 / P90")
                st.line_chart(tchart, x="valid_date", y=["temp_p10", "temp_p50", "temp_p90"], height=260)
                hchart = g2[["valid_date", "humidity_p10", "humidity_p50", "humidity_p90"]]
                st.caption("Humidity (%) — P10 / P50 / P90")
                st.line_chart(hchart, x="valid_date", y=["humidity_p10", "humidity_p50", "humidity_p90"], height=220)
            with st.expander("Raw weather rows (gold view)"):
                st.dataframe(sub, use_container_width=True, hide_index=True)

    with tab_risk:
        st.subheader("Crop risk (published run)")
        if risk_df.empty:
            st.info("`gold.v_dashboard_forecast_crop_risk` has no rows.")
        else:
            tier_order = {"none": 0, "watch": 1, "advisory": 2, "high": 3}
            risk_df = risk_df.copy()
            risk_df["_tier_ord"] = risk_df["risk_tier"].str.lower().map(lambda x: tier_order.get(str(x), 0))
            c1, c2 = st.columns(2)
            with c1:
                crops = sorted(risk_df["crop_name"].dropna().unique().tolist())
                crop_f = st.multiselect("Crops", crops, default=crops[: min(8, len(crops))])
            with c2:
                tiers = sorted(risk_df["risk_tier"].dropna().unique().tolist())
                tier_f = st.multiselect("Risk tier", tiers, default=tiers)
            filt = risk_df
            if crop_f:
                filt = filt[filt["crop_name"].isin(crop_f)]
            if tier_f:
                filt = filt[filt["risk_tier"].isin(tier_f)]
            st.dataframe(
                filt.drop(columns=["_tier_ord"], errors="ignore"),
                use_container_width=True,
                hide_index=True,
            )
            st.caption("Expand drivers JSON in your warehouse client if needed; full JSON is in the table above.")


if __name__ == "__main__":
    main()
