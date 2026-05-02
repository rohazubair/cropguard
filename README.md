# CropGuard

CropGuard pulls **hourly weather** from Open-Meteo, stores it per district, derives **crop-friendly temperature bands** from your CSV dataset, trains a **pooled hourly temperature forecaster**, and exposes a **REST API** that combines forecasts with those bands to label each crop’s future stress as **healthy**, **medium**, or **danger**.

---

## Big picture

1. **Ingest weather** — For each row in `DimDistrict`, call Open-Meteo (past + forecast window), optionally clean rows, upsert into `FactWeatherReadings`.
2. **Crop envelopes** — From `storage/Crop(Distric level).csv`, aggregate temperatures per crop (global) and per `(crop, district)`, then fill `FactCropTemperatureRange` and `FactCropDistrictHealthyTemp`.
3. **Train the model** — Concatenate hourly series from all districts, engineer lag/rolling/calendar features, predict **next-hour** temperature with **scikit-learn `HistGradientBoostingRegressor`**, save under `artifacts/`.
4. **Serve predictions** — The FastAPI app loads that artifact, runs **recursive multi-step** hourly forecasts for a chosen district, rolls them up **by calendar day**, and compares daily min/max to each crop’s bands to assign **health status**.

```mermaid
flowchart LR
    subgraph ingest [Ingestion]
        OM[Open-Meteo API]
        WV[weather_validation.py]
        DB[(PostgreSQL)]
        OM --> WV --> DB
    end
    subgraph crops [Crop metadata]
        CSV[Crop CSV]
        LT[load_crop_tables.py]
        CSV --> LT --> DB
    end
    subgraph ml [Forecasting]
        TR[train_temperature.py]
        ART[artifacts/*.joblib]
        TR --> ART
    end
    subgraph api [API]
        FA[FastAPI app]
        ART --> FA
        DB --> FA
        FA --> H[forecast-health JSON]
    end
```

---

## Repository layout

| Path | Role |
|------|------|
| `main.py` | Applies `storage/setup_db.sql`, reloads crop tables from CSV, then runs the Prefect weather pipeline. |
| `storage/setup_db.sql` | Creates schema `cropguard_dev` and tables: districts, weather readings, crops, global crop temperature range, district-specific healthy bands. |
| `storage/Crop(Distric level).csv` | Crop samples with `temperature`, `label`, `district`; used to derive optimal temperature corridors. |
| `ingestion/ingest_weather.py` | DB helpers, Open-Meteo fetch (`past_days` / `forecast_days`), writes `FactWeatherReadings` (hourly `temp_min`/`temp_max` mirror the same observation). |
| `ingestion/weather_validation.py` | Parses hourly JSON → DataFrame, bounds checks, spike damping, short gap fill for training/inference hygiene. |
| `ingestion/load_crop_tables.py` | Deletes/rebuilds `DimCrop` (+ cascaded facts), matches CSV district slugs to `lower(DimDistrict.name)`, inserts percentile-based min/max bands. |
| `orchestration/flow.py` | Prefect flow that loops districts and calls `fetch_weather` → `save_to_db`. |
| `prediction/features.py` | Per-district ordering, lags (`1`, `24`, `168`), rolling stats, cyclical time features; label = next hour (`forecast_target`). |
| `prediction/train_temperature.py` | Downloads pooled series from Open-Meteo for every district, trains the pipeline, writes `artifacts/temperature_model.joblib` + metrics JSON. |
| `prediction/forecast_temperature.py` | Loads the artifact, pulls fresh history for one district, **recursively** predicts N hours ahead (exo vars forward-filled after last observation). |
| `api/app.py` | FastAPI: districts list, model metrics path, **`/districts/{id}/forecast-health`**. |
| `api/health_engine.py` | Bucket hourly preds by **UTC date**; compare daily min/max to district/global bands → **healthy \| medium \| danger**. |

---

## Database model (conceptual)

- **`DimDistrict`** — Canonical districts with `lat` / `lon` (used for Open-Meteo and joins).
- **`FactWeatherReadings`** — One row per `(district_id, timestamp)` with temperature, precipitation, humidity, soil moisture.
- **`DimCrop`** — Distinct crop slug (e.g. `rice`) from the CSV `label` column.
- **`FactCropTemperatureRange`** — One row per crop: **global** empirical min/max °C (from all CSV rows for that crop).
- **`FactCropDistrictHealthyTemp`** — **Narrower** optimal band per `(crop_id, district_id)` where the CSV had rows for that district name (matched case-insensitively to `DimDistrict.name`).

Rows in the CSV whose `district` does not match any `DimDistrict` are counted as skipped in the loader summary.

---

## Environment

Create **`cropguard/.env`** (never commit real secrets):

```env
DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DATABASE?sslmode=require
DATABASE_SCHEMA=cropguard_dev
```

- **`DATABASE_SCHEMA`** must match the schema used in `storage/setup_db.sql` (`cropguard_dev`), so unqualified table names in Python resolve correctly after `SET search_path`.

For **CockroachDB Cloud** (or other managed Postgres), use the connection string they provide; **`sslmode=verify-full`** usually requires a CA file path (e.g. libpq `sslrootcert` or the driver’s SSL args). Local Docker Postgres often uses `sslmode=disable` or omits SSL.

---

## Quick start (local Postgres via Docker)

### 1. Prerequisites

- Python 3.10+ recommended  
- Docker (for optional local Postgres + pgAdmin from `docker-compose.yml`)

### 2. Python env

```bash
cd cropguard
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Start database (optional local)

```bash
docker compose up -d
```

Apply DDL (either pipe SQL or use app — see step 5):

```bash
docker exec -i cropguard_db psql -U admin -d cropguard < storage/setup_db.sql
```

Point **`.env`** `DATABASE_URL` at that instance (example host port **`5433`** from compose).

### 4. Orchestrated bootstrap + weather ingest

From `cropguard/` with `.env` set:

```bash
python main.py
```

This will:

1. Run every statement in **`storage/setup_db.sql`** (idempotent creates + seed districts).  
2. Run **`populate_crop_tables(truncate_existing=True)`** to rebuild crop bands from the CSV.  
3. Run the **Prefect** pipeline (`orchestration/flow.py`) to fetch weather for all districts and upsert **`FactWeatherReadings`**.

To run **only** the Prefect pipeline later:

```bash
python orchestration/flow.py
```

### 5. Train the temperature model

Needs network access (Open-Meteo) and valid `DimDistrict` coordinates:

```bash
python -m prediction.train_temperature --past-days 60
```

Produces:

- **`artifacts/temperature_model.joblib`** — fitted pipeline + metadata (gitignored pattern `artifacts/*.joblib`).  
- **`artifacts/temperature_model_metrics.json`** — holdout MAE/RMSE and per-district ingest notes.

If you see “not enough supervised samples”, increase **`--past-days`** or add more districts with coordinates.

### 6. Run the API

```bash
uvicorn api.app:app --reload --host 0.0.0.0 --port 8000
```

Useful endpoints:

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/healthz` | Liveness. |
| `GET` | `/districts` | All districts with ids and coordinates. |
| `GET` | `/model/temperature-metrics` | Last training metrics JSON (after training). |
| `GET` | `/districts/{district_id}/forecast-health?forecast_days=7` | Hourly model forecast + per-crop daily **healthy / medium / danger** (days 1–14). |

Example:

```http
GET http://127.0.0.1:8000/districts/1/forecast-health?forecast_days=7
```

The API returns, per crop, reference ranges, a **daily** array (predicted min/max/mean and status), and a **worst_case** day.

---

## How the model and health rules work (short)

- **Why HistGradientBoosting?** Hourly temperature is **tabular sequential**: lags, rolling means, and calendar signals are strong baselines; trees handle nonlinear interactions and missing patterns without a custom torch stack. The model is **pooled** across districts with **`district_id`** ordinally encoded in the same pipeline.
- **Target** — Each training row predicts **the temperature at the next hour** (`forecast_target`), so recursive forecasting appends predicted temps and recomputes features step by step.
- **Health labels** — For each future **day**, we take the **min** and **max** of predicted hourly temperatures. **danger** if outside the **global** crop band; **medium** if outside the **district** optimum (or near edges) but still inside global rules; **healthy** when comfortably inside the district band (or global if no district row exists). Exact thresholds live in `api/health_engine.py`.

---

## Prefect UI (optional)

```bash
prefect server start
```

Open `http://127.0.0.1:4200` to inspect flow runs when you use `orchestration/flow.py`.

---

## pgAdmin (local compose only)

If you use the bundled **pgAdmin** service, add a server pointing at the **Postgres** container (e.g. host `127.0.0.1`, port `5433`).  
**CockroachDB** is Postgres-wire compatible but **not** fully PostgreSQL-catalog compatible — GUI tools like pgAdmin may error on missing functions; prefer **psql**, **DBeaver**, or the vendor SQL shell for Cockroach.

---

## AI usage declaration (historical)

Tool: Gemini  
Used for: Early Docker/port debugging, schema and Prefect scaffolding  
Extent: Boilerplate and debugging assistance

Subsequent features (validation, ML, crop tables, FastAPI) are described in this README and the linked modules above.
