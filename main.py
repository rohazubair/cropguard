import sys

sys.dont_write_bytecode = True

from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_a, **_kw):  # pragma: no cover
        return False

load_dotenv(Path(__file__).resolve().parent / ".env")


def _apply_schema_sql():
    from ingestion.ingest_weather import connect_db

    sql_path = Path(__file__).resolve().parent / "storage" / "setup_db.sql"
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


def main():
    print("Applying database schema (idempotent)...")
    _apply_schema_sql()
    print("Seeding crop temperature envelopes from CSV (idempotent deletes + reload)...")
    from ingestion.load_crop_tables import populate_crop_tables

    print(populate_crop_tables(truncate_existing=True))
    print("Schema + crop lookups ready. Starting weather pipeline...")
    from orchestration.flow import weather_pipeline

    weather_pipeline()


if __name__ == "__main__":
    main()
