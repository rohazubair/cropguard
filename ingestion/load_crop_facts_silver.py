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

    def load_dotenv(*_a, **_kw):
        return False
load_dotenv(PROJECT_ROOT / '.env')
from ingestion.district_normalize import resolve_crop_district
from ingestion.districts_canonical import CANONICAL_CROP_DISTRICTS
from ingestion.ingest_weather import _configure_search_path, connect_db
from ingestion.silver_ge_validation import validate_silver_crop_facts_df
SILVER_CROP_FIELD_PLAN: tuple[tuple[str, str], ...] = (('temperature', 'temperature'), ('humidity', 'humidity'), ('label', 'crop_name'))
_NUMERIC_SILVER = ('temperature', 'humidity')

def _fetch_district_maps(cur) -> tuple[dict[str, int], dict[int, str]]:
    cur.execute('SELECT district_id, name FROM bronze.districts_dim')
    rows = cur.fetchall()
    name_to_id: dict[str, int] = {}
    id_to_name: dict[int, str] = {}
    for did, name in rows:
        if name is None:
            continue
        key = str(name).strip().lower()
        i = int(did)
        name_to_id[key] = i
        id_to_name[i] = str(name).strip()
    return (name_to_id, id_to_name)

def _required_district_ids(name_to_id: dict[str, int]) -> frozenset[int]:
    ids: list[int] = []
    for c in CANONICAL_CROP_DISTRICTS:
        k = c.strip().lower()
        if k in name_to_id:
            ids.append(name_to_id[k])
    return frozenset(ids)

def _tiered_median_impute(df: pd.DataFrame, col: str) -> tuple[pd.Series, int]:
    before_num = pd.to_numeric(df[col], errors='coerce')
    pre_null = before_num.isna()
    s = before_num.copy()
    s = s.groupby([df['crop_name'], df['district_id']], dropna=False).transform(lambda x: x.fillna(x.median()))
    s = s.groupby(df['crop_name'], dropna=False).transform(lambda x: x.fillna(x.median()))
    gmed = s.median()
    if pd.isna(gmed):
        gmed = 0.0
    s = s.fillna(gmed)
    imputed = int((pre_null & s.notna()).sum())
    return (s, imputed)

def _prepare_silver_frame(rows: list[tuple[Any, ...]], name_to_id: dict[str, int], id_to_name: dict[int, str]) -> tuple[pd.DataFrame, dict[str, int]]:
    dropped_unmapped = 0
    dropped_no_dim_id = 0
    records: list[dict[str, Any]] = []
    for row in rows:
        _bronze_id, temperature, humidity, label, district_raw = row
        canon = resolve_crop_district(district_raw)
        if canon is None:
            dropped_unmapped += 1
            continue
        key = canon.strip().lower()
        district_id = name_to_id.get(key)
        if district_id is None:
            dropped_no_dim_id += 1
            continue
        official = id_to_name.get(district_id, canon)
        records.append({'district_id': district_id, 'temperature': temperature, 'humidity': humidity, 'crop_name': label, 'district': official})
    df = pd.DataFrame.from_records(records, columns=['district_id', 'temperature', 'humidity', 'crop_name', 'district'])
    if df.empty:
        return (df, {'dropped_unmapped': dropped_unmapped, 'dropped_no_dim_id': dropped_no_dim_id, 'dropped_empty_label': 0})
    n_after_map = len(df)
    df = df.copy()
    df['crop_name'] = df['crop_name'].astype(str).str.strip()
    df = df[df['crop_name'] != '']
    dropped_empty_label = n_after_map - len(df)
    return (df, {'dropped_unmapped': dropped_unmapped, 'dropped_no_dim_id': dropped_no_dim_id, 'dropped_empty_label': dropped_empty_label})

def _crops_with_all_districts(df: pd.DataFrame, required_ids: frozenset[int]) -> set[str]:
    if df.empty or not required_ids:
        return set()
    out: set[str] = set()
    for crop, g in df.groupby('crop_name', sort=False):
        if required_ids <= set(g['district_id'].astype(int).unique()):
            out.add(str(crop))
    return out

def load_crop_facts_silver(*, truncate_before_load: bool=True) -> dict[str, Any]:
    rows: list[Any] = []
    out_rows: list[tuple[Any, ...]] = []
    prep_stats: dict[str, int] = {}
    imputed_temperature = 0
    imputed_humidity = 0
    before_crop_count = 0
    after_crop_count = 0
    rows_dropped_crop_filter = 0
    rows_removed_coverage = 0
    required_ids: frozenset[int] = frozenset()
    missing_canonical: list[str] = []
    ge_crop: dict[str, Any] = {}
    bronze_cols = ', '.join((b for b, _ in SILVER_CROP_FIELD_PLAN)) + ', district'
    select_sql = f'SELECT id, {bronze_cols} FROM bronze.crop_facts'
    conn = connect_db()
    try:
        cur = conn.cursor()
        _configure_search_path(cur)
        name_to_id, id_to_name = _fetch_district_maps(cur)
        required_ids = _required_district_ids(name_to_id)
        missing_canonical = [c for c in CANONICAL_CROP_DISTRICTS if c.strip().lower() not in name_to_id]
        cur.execute(select_sql)
        rows = cur.fetchall()
        df, prep_stats = _prepare_silver_frame(rows, name_to_id, id_to_name)
        if not df.empty:
            for col in _NUMERIC_SILVER:
                df[col], n_imp = _tiered_median_impute(df, col)
                if col == 'temperature':
                    imputed_temperature = n_imp
                else:
                    imputed_humidity = n_imp
            before_crop_count = int(df['crop_name'].nunique())
            rows_before_coverage = len(df)
            keep_crops = _crops_with_all_districts(df, required_ids)
            df = df[df['crop_name'].isin(keep_crops)].copy()
            after_crop_count = int(df['crop_name'].nunique())
            rows_dropped_crop_filter = before_crop_count - after_crop_count
            rows_removed_coverage = rows_before_coverage - len(df)
            df = df.dropna(subset=list(_NUMERIC_SILVER) + ['district_id'])
            df = df[np.isfinite(df['temperature']) & np.isfinite(df['humidity'])]
        if not df.empty:
            ge_crop = validate_silver_crop_facts_df(df)
        out_rows = list(zip(df['district_id'].astype(int), df['temperature'].astype(float), df['humidity'].astype(float), df['crop_name'].astype(str), df['district'].astype(str))) if not df.empty else []
        insert_cols = 'district_id, ' + ', '.join((s for _, s in SILVER_CROP_FIELD_PLAN)) + ', district'
        insert_sql = f'\n            INSERT INTO silver.crop_facts ({insert_cols})\n            VALUES (%s, %s, %s, %s, %s)\n            '
        if truncate_before_load:
            cur.execute('TRUNCATE TABLE silver.crop_facts')
        if out_rows:
            execute_batch(cur, insert_sql, out_rows, page_size=500)
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {'bronze_rows_read': len(rows), 'rows_inserted': len(out_rows), 'rows_dropped_unmapped': prep_stats.get('dropped_unmapped', 0), 'rows_dropped_no_dim_id': prep_stats.get('dropped_no_dim_id', 0), 'rows_dropped_empty_crop_name': prep_stats.get('dropped_empty_label', 0), 'required_district_ids_count': len(required_ids), 'canonical_districts_missing_from_dim': missing_canonical, 'crops_before_full_coverage_filter': before_crop_count, 'crops_after_full_coverage_filter': after_crop_count, 'crops_removed_missing_districts': rows_dropped_crop_filter, 'rows_removed_missing_district_coverage': rows_removed_coverage, 'imputed_temperature_cells': imputed_temperature, 'imputed_humidity_cells': imputed_humidity, 'silver_fields': ['district_id'] + [s for _, s in SILVER_CROP_FIELD_PLAN] + ['district'], 'great_expectations': {'silver_crop_facts': ge_crop}}
