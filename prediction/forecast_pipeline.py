from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import Any
import joblib
import pandas as pd
from ingestion.apply_bronze_schema import apply_bronze_schema
from ingestion.ingest_weather import _configure_search_path
from prediction.forecast_constants import HORIZON_DAYS, LOOKBACK_DAYS, MODEL_NAME_WEATHER_DAILY
from prediction.forecast_daily_dataset import hourly_to_daily
from prediction.forecast_db import db_conn, finish_run, get_active_model, insert_crop_risk_points, insert_model_registry, insert_run, insert_weather_points, publish_run, refresh_gold_dashboard, utc_now_naive
from prediction.forecast_drift import should_retrain_from_drift
from prediction.forecast_risk import crop_envelopes, score_crop_day, valid_dates_from_anchor
from prediction.forecast_train import train_from_silver_hourly

def _load_hourly(cur) -> pd.DataFrame:
    cur.execute('\n        SELECT district_id, observed_ts, temperature, humidity_pct\n        FROM silver.weather_readings_fact\n        ORDER BY district_id, observed_ts\n        ')
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=['district_id', 'observed_ts', 'temperature', 'humidity_pct'])

def _load_crop_silver(cur) -> pd.DataFrame:
    cur.execute('\n        SELECT district_id, crop_name, temperature, humidity\n        FROM silver.crop_facts\n        WHERE district_id IS NOT NULL\n        ')
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=['district_id', 'crop_name', 'temperature', 'humidity'])

def _inference_feature_rows(daily: pd.DataFrame, feature_cols: list[str]) -> list[tuple[int, pd.Timestamp, dict[str, float | int]]]:
    tcols = [f't_{i}' for i in range(LOOKBACK_DAYS)]
    hcols = [f'h_{i}' for i in range(LOOKBACK_DAYS)]
    out: list[tuple[int, pd.Timestamp, dict[str, float | int]]] = []
    for did, grp in daily.groupby('district_id', sort=False):
        grp = grp.sort_values('date').reset_index(drop=True)
        if len(grp) < LOOKBACK_DAYS:
            continue
        window = grp.iloc[-LOOKBACK_DAYS:]
        anchor = pd.Timestamp(window['date'].iloc[-1]).normalize()
        row: dict[str, float | int] = {'district_id': int(did)}
        for j in range(LOOKBACK_DAYS):
            row[tcols[j]] = float(window['temp_max'].iloc[j])
            row[hcols[j]] = float(window['humidity_mean'].iloc[j])
        out.append((int(did), anchor, row))
    return out

def _load_bundle(artifact_uri: str) -> dict[str, Any]:
    payload = joblib.load(artifact_uri)
    return payload['bundle']

def run_forecast_pipeline(*, project_root: Path | None=None) -> dict[str, Any]:
    root = project_root or Path(__file__).resolve().parents[1]
    apply_bronze_schema(project_root=root)
    summary: dict[str, Any] = {'retrained': False, 'published_run_id': None}
    conn = db_conn()
    try:
        cur = conn.cursor()
        _configure_search_path(cur)
        hourly = _load_hourly(cur)
        if hourly.empty:
            summary.update({'status': 'skipped', 'reason': 'no_silver_weather'})
            conn.commit()
            cur.close()
            return summary
        daily = hourly_to_daily(hourly)
        need_days = LOOKBACK_DAYS + HORIZON_DAYS
        if daily['district_id'].nunique() < 1 or len(daily) < need_days:
            summary.update({'status': 'skipped', 'reason': 'insufficient_daily_history'})
            conn.commit()
            cur.close()
            return summary
        active = get_active_model(cur, MODEL_NAME_WEATHER_DAILY)
        drift_ref: dict[str, Any] = {}
        retrain = active is None
        if active:
            drift_ref = active.get('feature_reference_json') or {}
            if isinstance(drift_ref, str):
                import json
                drift_ref = json.loads(drift_ref)
            retrain = should_retrain_from_drift(daily, drift_ref)
        if retrain:
            try:
                _bundle, metrics, version, uri = train_from_silver_hourly(hourly)
                ref = _bundle.get('drift_reference', {})
                train_end = hourly['observed_ts'].max()
                if isinstance(train_end, pd.Timestamp):
                    train_end_ts = train_end.to_pydatetime()
                else:
                    train_end_ts = datetime.fromisoformat(str(train_end)[:19])
                insert_model_registry(cur, model_name=MODEL_NAME_WEATHER_DAILY, version=version, artifact_uri=uri, train_data_end_ts=train_end_ts, metrics=metrics, feature_reference=ref, set_active=True)
                summary['retrained'] = True
                summary['model_version'] = version
            except ValueError as e:
                summary.update({'status': 'error', 'reason': str(e)})
                conn.commit()
                cur.close()
                return summary
        active2 = get_active_model(cur, MODEL_NAME_WEATHER_DAILY)
        if not active2:
            summary.update({'status': 'error', 'reason': 'no_active_model_after_train'})
            conn.commit()
            cur.close()
            return summary
        bundle = _load_bundle(active2['artifact_uri'])
        feature_cols: list[str] = list(bundle['feature_cols'])
        pipes_t = bundle['pipelines_temp']
        pipes_h = bundle['pipelines_humidity']
        input_until = pd.to_datetime(hourly['observed_ts']).max()
        ts = pd.Timestamp(input_until)
        if ts.tzinfo is not None:
            input_until_naive = ts.tz_convert('UTC').to_pydatetime().replace(tzinfo=None)
        else:
            input_until_naive = ts.to_pydatetime()
        run_id = insert_run(cur, model_id=int(active2['model_id']), input_weather_until=input_until_naive, horizon_days=HORIZON_DAYS, granularity='daily')
        weather_rows: list[tuple[Any, ...]] = []
        inf_rows = _inference_feature_rows(daily, feature_cols)
        for did, anchor, rowdict in inf_rows:
            X1 = pd.DataFrame([rowdict])[feature_cols]
            t10 = pipes_t[0.1].predict(X1)[0]
            t50 = pipes_t[0.5].predict(X1)[0]
            t90 = pipes_t[0.9].predict(X1)[0]
            h10 = pipes_h[0.1].predict(X1)[0]
            h50 = pipes_h[0.5].predict(X1)[0]
            h90 = pipes_h[0.9].predict(X1)[0]
            vdates = valid_dates_from_anchor(anchor, HORIZON_DAYS)
            for k in range(HORIZON_DAYS):
                weather_rows.append((run_id, did, vdates[k], k + 1, float(t10[k]), float(t50[k]), float(t90[k]), float(h10[k]), float(h50[k]), float(h90[k])))
        insert_weather_points(cur, run_id, weather_rows)
        crop_df = _load_crop_silver(cur)
        env_map = crop_envelopes(crop_df)
        risk_rows: list[tuple[Any, ...]] = []
        for wr in weather_rows:
            _, wr_did, vdate, _, _t10, tp50, _t90, _h10, hp50, _h90 = wr
            for (e_did, crop), env in env_map.items():
                if e_did != wr_did:
                    continue
                score, tier, drivers = score_crop_day(tp50, hp50, env)
                risk_rows.append((run_id, wr_did, crop, vdate, score, tier, drivers))
        insert_crop_risk_points(cur, run_id, risk_rows)
        publish_run(cur, run_id)
        finish_run(cur, run_id, status='success', drift_checked=True, drift_retrain_triggered=summary.get('retrained', False), notes=None)
        refresh_gold_dashboard(cur)
        conn.commit()
        cur.close()
        summary.update({'status': 'success', 'published_run_id': run_id, 'weather_points': len(weather_rows), 'crop_risk_points': len(risk_rows), 'model_id': int(active2['model_id'])})
        return summary
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
