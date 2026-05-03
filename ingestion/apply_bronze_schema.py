from __future__ import annotations
from pathlib import Path

_GOLD_DASHBOARD_REL_NAMES = ('v_dashboard_forecast_weather', 'v_dashboard_forecast_crop_risk', 'v_dashboard_published_forecast_run')


def _drop_legacy_gold_views(cur) -> None:
    from psycopg2 import sql as psql
    cur.execute(
        '\n        SELECT table_name\n        FROM information_schema.views\n        WHERE table_schema = %s\n          AND table_name IN %s\n        ',
        ('gold', _GOLD_DASHBOARD_REL_NAMES),
    )
    for (tname,) in cur.fetchall():
        cur.execute(psql.SQL('DROP VIEW IF EXISTS {}.{} CASCADE').format(psql.Identifier('gold'), psql.Identifier(tname)))


def apply_bronze_schema(project_root: Path | None=None) -> None:
    from ingestion.ingest_weather import connect_db
    root = project_root or Path(__file__).resolve().parents[1]
    sql_path = root / 'storage' / 'setup_db.sql'
    raw = sql_path.read_text(encoding='utf-8')
    statements = [s.strip() for s in raw.split(';') if s.strip()]
    conn = connect_db()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        _drop_legacy_gold_views(cur)
        for stmt in statements:
            cur.execute(stmt)
        cur.close()
    finally:
        conn.close()
