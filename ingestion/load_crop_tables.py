"""Populate DimCrop plus temperature envelope tables using the Crop(Distric level).csv snapshot."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

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


def _slug(text: str) -> str:
    return str(text or "").strip().lower()


def _summarize_temperature(series: pd.Series) -> pd.Series:
    return pd.Series(
        {
            "pct10": float(series.quantile(0.10)),
            "pct90": float(series.quantile(0.90)),
            "samples": int(len(series)),
        }
    )


def populate_crop_tables(csv_path: Path | None = None, *, truncate_existing: bool = True) -> dict[str, int]:
    csv_path = csv_path or (PROJECT_ROOT / "storage" / "Crop(Distric level).csv")

    load_dotenv(PROJECT_ROOT / ".env")

    conn = connect_db()
    inserted_crops = 0
    inserted_global_rows = 0
    inserted_district_rows = 0
    skipped_rows = 0

    try:
        cur = conn.cursor()
        _configure_search_path(cur)

        df = pd.read_csv(csv_path)
        column_lookup = {_slug(col): col for col in df.columns}
        required = {"label", "temperature", "district"}
        if not required.issubset(column_lookup.keys()):
            missing = sorted(required - set(column_lookup.keys()))
            raise ValueError(f"CSV missing expected columns: {missing}")

        label_series = df[column_lookup["label"]].astype(str, copy=False).map(_slug)
        district_series = df[column_lookup["district"]].astype(str, copy=False).map(_slug)

        staging = pd.DataFrame(
            {
                "crop_slug": label_series,
                "temperature": pd.to_numeric(df[column_lookup["temperature"]], errors="coerce"),
                "district_slug": district_series,
            }
        )
        staging = staging.dropna(subset=["crop_slug", "temperature", "district_slug"])

        cur.execute(
            """
            SELECT lower(trim(name))
            FROM DimDistrict
            """
        )
        valid_district_slugs = {row[0] for row in cur.fetchall() if row[0]}
        before_filter = len(staging)
        matched = staging[staging["district_slug"].isin(valid_district_slugs)].copy()

        skipped_rows += int(before_filter - len(matched))

        if truncate_existing:
            cur.execute("DELETE FROM DimCrop")

        unique_crops = sorted(matched["crop_slug"].unique())
        slug_to_crop_id: dict[str, int] = {}

        for crop_slug in unique_crops:
            cur.execute(
                "INSERT INTO DimCrop (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                (crop_slug,),
            )
            cur.execute("SELECT crop_id FROM DimCrop WHERE name = %s", (crop_slug,))
            slug_to_crop_id[crop_slug] = int(cur.fetchone()[0])

        inserted_crops = len(unique_crops)

        for crop_slug, series in matched.groupby("crop_slug")["temperature"]:
            stats = _summarize_temperature(series)
            crop_id = slug_to_crop_id[crop_slug]
            pct10 = float(stats["pct10"])
            pct90 = float(stats["pct90"])
            samples = int(stats["samples"])
            span = max((pct90 - pct10) * 0.08, 0.5)
            lo = round(max(pct10 - span, -50.0), 2)
            hi = round(min(pct90 + span, 60.0), 2)

            cur.execute(
                """
                INSERT INTO FactCropTemperatureRange (crop_id, temp_min_c, temp_max_c, sample_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (crop_id) DO UPDATE
                SET temp_min_c = EXCLUDED.temp_min_c,
                    temp_max_c = EXCLUDED.temp_max_c,
                    sample_count = EXCLUDED.sample_count,
                    updated_at = now()
                """,
                (crop_id, lo, hi, samples),
            )
            inserted_global_rows += 1

        for (crop_slug, district_slug), series in matched.groupby(["crop_slug", "district_slug"])[
            "temperature"
        ]:
            stats = _summarize_temperature(series)
            pct10 = float(stats["pct10"])
            pct90 = float(stats["pct90"])
            samples = int(stats["samples"])
            crop_id = slug_to_crop_id[crop_slug]

            span = max((pct90 - pct10) * 0.06, 0.4)
            lo = round(max(pct10 - span, -50.0), 2)
            hi = round(min(pct90 + span, 60.0), 2)

            cur.execute(
                """
                INSERT INTO FactCropDistrictHealthyTemp (crop_id, district_id, temp_min_c, temp_max_c, sample_count)
                SELECT %s, d.district_id, %s, %s, %s
                FROM DimDistrict d
                WHERE lower(trim(d.name)) = %s
                ON CONFLICT (crop_id, district_id) DO UPDATE
                SET temp_min_c = EXCLUDED.temp_min_c,
                    temp_max_c = EXCLUDED.temp_max_c,
                    sample_count = EXCLUDED.sample_count,
                    updated_at = now()
                """,
                (crop_id, lo, hi, samples, district_slug),
            )
            if cur.rowcount:
                inserted_district_rows += 1

        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "crops_inserted_or_refreshed": inserted_crops,
        "district_envelopes": inserted_district_rows,
        "global_envelopes": inserted_global_rows,
        "skipped_unmatched_samples": skipped_rows,
    }


if __name__ == "__main__":
    summary = populate_crop_tables(truncate_existing=True)
    print(summary)
