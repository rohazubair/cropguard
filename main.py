import sys

sys.dont_write_bytecode = True

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def _apply_schema_sql():
    import psycopg2

    from ingestion.ingest_weather import _database_url

    sql_path = Path(__file__).resolve().parent / "storage" / "setup_db.sql"
    raw = sql_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in raw.split(";") if s.strip()]
    conn = psycopg2.connect(_database_url())
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
    print("Schema ready. Starting pipeline...")
    from orchestration.flow import weather_pipeline

    weather_pipeline()


if __name__ == "__main__":
    main()
