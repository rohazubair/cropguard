"""Load Crop(Distric level).csv into bronze.crop_facts with no transforms (raw values)."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import errorcodes
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

CSV_COLUMNS = ["N", "P", "K", "temperature", "humidity", "ph", "rainfall", "label", "district"]
CROP_CSV_STATE_KEY = "crop_csv_sha256"


def load_crop_facts_bronze(
    csv_path: Path | None = None,
    *,
    truncate_before_load: bool = True,
    skip_if_csv_unchanged: bool = True,
) -> dict[str, int | bool | str]:
    """
    Read the crop CSV as-is and insert into bronze.crop_facts.

    If skip_if_csv_unchanged and bronze.ingestion_state matches file SHA-256, skips load.
    """
    csv_path = csv_path or (PROJECT_ROOT / "storage" / "Crop(Distric level).csv")

    file_bytes = csv_path.read_bytes()
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    conn = connect_db()
    try:
        cur = conn.cursor()
        _configure_search_path(cur)

        if skip_if_csv_unchanged:
            stored_row = None
            try:
                cur.execute(
                    "SELECT content_hash FROM bronze.ingestion_state WHERE key = %s",
                    (CROP_CSV_STATE_KEY,),
                )
                stored_row = cur.fetchone()
            except psycopg2.Error as exc:
                conn.rollback()
                if getattr(exc, "pgcode", None) != errorcodes.UNDEFINED_TABLE:
                    raise
            if stored_row and stored_row[0] == file_hash:
                cur.close()
                return {
                    "skipped": True,
                    "reason": "crop_csv_unchanged",
                    "content_hash": file_hash,
                    "rows_inserted": 0,
                }

        df = pd.read_csv(Path(csv_path))
        missing = [c for c in CSV_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"CSV missing columns: {missing}")

        df = df[CSV_COLUMNS].copy()
        for c in ("N", "P", "K"):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        for c in ("temperature", "humidity", "ph", "rainfall"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        for c in ("label", "district"):
            df[c] = df[c].astype(str)

        bad = df[df.isna().any(axis=1)]
        if not bad.empty:
            raise ValueError(f"CSV has {len(bad)} row(s) with null/invalid numbers after read.")

        rows = list(
            zip(
                df["N"].astype(int),
                df["P"].astype(int),
                df["K"].astype(int),
                df["temperature"].astype(float),
                df["humidity"].astype(float),
                df["ph"].astype(float),
                df["rainfall"].astype(float),
                df["label"],
                df["district"],
            )
        )

        insert_sql = """
            INSERT INTO bronze.crop_facts (
                n, p, k, temperature, humidity, ph, rainfall, label, district
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        if truncate_before_load:
            cur.execute("TRUNCATE TABLE bronze.crop_facts")
        if rows:
            execute_batch(cur, insert_sql, rows, page_size=500)

        cur.execute(
            """
            INSERT INTO bronze.ingestion_state (key, content_hash)
            VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE
            SET content_hash = EXCLUDED.content_hash, updated_at = now()
            """,
            (CROP_CSV_STATE_KEY, file_hash),
        )

        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "skipped": False,
        "content_hash": file_hash,
        "rows_inserted": len(rows),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load crop CSV into bronze.crop_facts")
    parser.add_argument("--csv", type=str, default="", help="Path to CSV (default: storage/Crop(Distric level).csv)")
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Append without truncating (may duplicate rows)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reload even if CSV hash unchanged",
    )
    args = parser.parse_args()
    path = Path(args.csv).resolve() if args.csv else None
    out = load_crop_facts_bronze(
        path,
        truncate_before_load=not args.no_truncate,
        skip_if_csv_unchanged=not args.force,
    )
    print(out)
