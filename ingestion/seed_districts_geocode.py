from __future__ import annotations
import json
import sys
import time
from pathlib import Path
from typing import Any
import requests
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
try:
    from dotenv import load_dotenv
except ImportError:

    def load_dotenv(*_a, **_kw):
        return False
load_dotenv(PROJECT_ROOT / '.env')
from ingestion.districts_canonical import CANONICAL_CROP_DISTRICTS
from ingestion.ingest_weather import _configure_search_path, connect_db
NOMINATIM_SEARCH = 'https://nominatim.openstreetmap.org/search'
NOMINATIM_USER_AGENT = 'CropGuard/1.0 (district seed; https://github.com/)'
PUNJAB_DISTRICTS = CANONICAL_CROP_DISTRICTS

def _district_exists(cur, name: str) -> bool:
    cur.execute('\n        SELECT 1\n        FROM bronze.districts_dim\n        WHERE lower(trim(name)) = lower(trim(%s))\n        LIMIT 1\n        ', (name,))
    return cur.fetchone() is not None

def _nominatim_query(display_name: str) -> str:
    if display_name == 'Islamabad':
        return 'Islamabad, Pakistan'
    return f'{display_name}, Punjab, Pakistan'

def _nominatim_lookup(display_name: str, *, delay_s: float) -> tuple[float, float] | None:
    time.sleep(delay_s)
    params = {'q': _nominatim_query(display_name), 'format': 'json', 'limit': 1}
    headers = {'User-Agent': NOMINATIM_USER_AGENT}
    r = requests.get(NOMINATIM_SEARCH, params=params, headers=headers, timeout=45)
    r.raise_for_status()
    data = r.json()
    if not data:
        return None
    return (float(data[0]['lat']), float(data[0]['lon']))

def seed_missing_districts(*, delay_s: float=1.1) -> dict[str, Any]:
    inserted: list[str] = []
    skipped_existing: list[str] = []
    geocode_failed: list[str] = []
    conn = connect_db()
    try:
        cur = conn.cursor()
        _configure_search_path(cur)
        for name in PUNJAB_DISTRICTS:
            if _district_exists(cur, name):
                skipped_existing.append(name)
                continue
            coords = _nominatim_lookup(name, delay_s=delay_s)
            if coords is None:
                geocode_failed.append(name)
                continue
            lat, lon = coords
            cur.execute('\n                INSERT INTO bronze.districts_dim (name, lat, lon)\n                VALUES (%s, %s, %s)\n                ', (name, round(lat, 6), round(lon, 6)))
            inserted.append(name)
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {'inserted': inserted, 'skipped_existing': skipped_existing, 'geocode_failed': geocode_failed, 'nominatim_delay_s': delay_s}
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Geocode missing districts into bronze.districts_dim')
    p.add_argument('--delay', type=float, default=1.1, help='Seconds between Nominatim requests (policy)')
    args = p.parse_args()
    print(json.dumps(seed_missing_districts(delay_s=args.delay), indent=2))
