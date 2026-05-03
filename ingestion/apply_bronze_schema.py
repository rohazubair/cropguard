"""Apply DDL from storage/setup_db.sql (bronze schema)."""

from __future__ import annotations

from pathlib import Path


def apply_bronze_schema(project_root: Path | None = None) -> None:
    """Run each semicolon-separated statement in setup_db.sql with autocommit."""
    from ingestion.ingest_weather import connect_db

    root = project_root or Path(__file__).resolve().parents[1]
    sql_path = root / "storage" / "setup_db.sql"
    raw = sql_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in raw.split(";") if s.strip()]
    conn = connect_db()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
        cur.close()
    finally:
        conn.close()
