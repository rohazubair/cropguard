# CropGuard

CropGuard ingests **crop CSV** and **hourly weather** (Open-Meteo) into **PostgreSQL** (`bronze` → `silver`), runs a **drift-gated daily weather forecast** plus **crop risk** scoring into **`silver.ml_*`**, and exposes **gold** dashboard views. **Prefect** orchestrates the pipeline (`main.py`); **FastAPI** serves districts and dashboard JSON; **Streamlit** reads the API.

---

## Data flow (short)

1. **DDL** — `storage/setup_db.sql` defines `bronze`, `silver`, `gold` (tables + `gold.v_dashboard_*` views).
2. **Ingest** — District geocode seed, crop CSV → `bronze.crop_facts` → `silver.crop_facts` (with Great Expectations checks), Open-Meteo → `bronze.weather_readings_fact` → `silver.weather_readings_fact`.
3. **Forecast** — `prediction.forecast_pipeline` trains/serves a multi-output daily model, writes forecasts and risk to `silver.ml_*`, publishes a default run, **gold** views read the published run.
4. **UI** — `uvicorn api.app:app` then `streamlit run dashboard/app.py` (set `CROPGUARD_API_BASE` if the API is not on `http://127.0.0.1:8000`).

```mermaid
flowchart LR
    CSV[Crop CSV] --> Bronze[bronze.crop_facts]
    OM[Open-Meteo] --> BronzeW[bronze.weather_readings_fact]
    Bronze --> Silver[silver.crop_facts / silver.weather_readings_fact]
    BronzeW --> Silver
    Silver --> ML[prediction.forecast_pipeline]
    ML --> SilverML[silver.ml_*]
    SilverML --> Gold[gold.v_dashboard_*]
    API[FastAPI /dashboard] --> Gold
    ST[Streamlit] --> API
```

---

## Repository layout

| Path | Role |
|------|------|
| `main.py` | Runs Prefect bronze pipeline or `--forecast-only` forecast flow. |
| `storage/setup_db.sql` | Bronze/silver/gold DDL and dashboard views. |
| `orchestration/flow.py` | Prefect flow: schema → seed → crop → silver → weather → forecast. |
| `orchestration/forecast_flow.py` | Prefect flow: forecast pipeline only. |
| `ingestion/*` | CSV loaders, weather ingest, silver GE validation, district helpers. |
| `prediction/forecast_pipeline.py` | Drift check, optional retrain, infer, publish to `silver.ml_*`. |
| `prediction/forecast_train.py` | Daily multi-output model training from silver hourly weather. |
| `prediction/forecast_db.py` | Inserts into `silver.ml_*` tables. |
| `prediction/forecast_risk.py` | Risk tiers from crop facts + forecast bands. |
| `api/app.py` | FastAPI: `/healthz`, `/districts`, `/dashboard/*`. |
| `api/dashboard_routes.py` | JSON from `gold.v_dashboard_*`. |
| `dashboard/app.py` | Streamlit charts (HTTP client to API). |
| `Dockerfile` | Python 3.11 image + **ca-certificates** (TLS to cloud DBs); API default CMD; compose overrides for Streamlit / pipeline. |
| `.env.example` | Template for **`DATABASE_URL`** (copy to **`.env`**). |
| `docker-compose.yml` | FastAPI + Streamlit; **`env_file: .env`** (same **`DATABASE_URL`** as host); optional **pgadmin** and **pipeline** profiles; **`cropguard_artifacts`** volume for pipeline. |

---

## Run with Docker

From **`cropguard/`** with [Docker Compose](https://docs.docker.com/compose/) installed.

**Same database as on the host:** put **`DATABASE_URL`** in **`.env`** (same file you use for `python main.py`). Compose injects that file into **api** and **pipeline** via **`env_file: .env`**. There is **no bundled Postgres** in this compose file—the containers use your URL (e.g. Cockroach Cloud) exactly like a local run.

Copy **`.env.example`** → **`.env`** if you do not have one yet, then set **`DATABASE_URL`**.

**Ports:** API **8000**, Streamlit **8501**. Streamlit always calls the API at **`http://api:8000`** inside Docker (compose **`environment`** overrides any host-only **`CROPGUARD_API_BASE`** in **`.env`** for that service).

**TLS:** The image installs **`ca-certificates`** so **`sslmode=verify-full`** against managed providers usually works. If your provider requires a custom CA, add **`sslrootcert=...`** to **`DATABASE_URL`** (path must be valid *inside* the container, e.g. mount a file under **`/app/certs`** and reference it).

1. **`.env`** with **`DATABASE_URL`** set (required).

2. **Build and start API + Streamlit**

```bash
docker compose build
docker compose up -d
```

3. **Run the full Prefect pipeline once** (ingest + forecast; needs outbound internet for Open-Meteo and geocoding). Trained model artifacts are stored in the **`cropguard_artifacts`** volume.

```bash
docker compose --profile pipeline run --rm pipeline
```

4. **Forecast only** (after data exists)

```bash
docker compose --profile pipeline run --rm pipeline python main.py --forecast-only
```

5. **Open in the browser**

- API docs: `http://127.0.0.1:8000/docs`
- Dashboard UI: `http://127.0.0.1:8501`

6. **Optional — pgAdmin** (does not auto-configure your server; add a server in the UI using the host/port/user from your **`DATABASE_URL`**)

```bash
docker compose --profile tools up -d
```

Then open `http://127.0.0.1:8080` (default login **`admin@admin.com`** / **`admin`** unless you changed it in compose).

7. **Stop containers**

```bash
docker compose down
```

Add **`-v`** only if you want to remove the **ML artifact** volume **`cropguard_artifacts`** (your cloud database is unchanged).

---

## Environment

Create **`cropguard/.env`** (see **`.env.example`**). At minimum:

```env
DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DATABASE?sslmode=require
```

**Docker:** **`api`** and **`pipeline`** load this file unchanged, so behaviour matches **`python main.py`** on the host against the same database.

Optional for **host** Streamlit when the API is not on port 8000:

```env
CROPGUARD_API_BASE=http://127.0.0.1:8000
```

---

## Quick start

```bash
cd cropguard
python -m venv .venv
# Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Apply DDL (or rely on `python main.py`, which runs `apply_bronze_schema` first):

```bash
# example: psql < storage/setup_db.sql
```

Run the full pipeline:

```bash
python main.py
```

Forecast only (after bronze/silver data exists):

```bash
python main.py --forecast-only
```

**API**

```bash
uvicorn api.app:app --reload --host 0.0.0.0 --port 8000
```

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/healthz` | Liveness. |
| `GET` | `/districts` | Districts from `bronze.districts_dim`. |
| `GET` | `/dashboard/published-run` | Published ML run metadata (gold view). |
| `GET` | `/dashboard/forecast-weather` | Weather fan charts data source. |
| `GET` | `/dashboard/forecast-crop-risk` | Crop risk rows. |

**Streamlit** (with API running):

```bash
streamlit run dashboard/app.py
```

---

## Verify everything (suggested order)

Do this from the **`cropguard`** directory (after Quick start: venv, `pip install`, `.env` with **`DATABASE_URL`**).

1. **Full pipeline** — `python main.py` (applies DDL via `apply_bronze_schema`, then Prefect ingest + forecast).
2. **Forecast only** *(optional)* — `python main.py --forecast-only` once bronze/silver data already exists.
3. **API** — second terminal, same venv: `uvicorn api.app:app --reload --host 0.0.0.0 --port 8000`
4. **HTTP checks** — browser `http://127.0.0.1:8000/docs`, or for example:

```bash
curl -s http://127.0.0.1:8000/healthz
curl -s http://127.0.0.1:8000/districts
curl -s http://127.0.0.1:8000/dashboard/published-run
```

On Windows PowerShell you can use `Invoke-RestMethod http://127.0.0.1:8000/healthz` (same paths as above).

5. **Streamlit** — third terminal (keep **uvicorn** running): `streamlit run dashboard/app.py` — use the URL Streamlit prints. Set **`CROPGUARD_API_BASE`** if the API is not on port **8000**.
6. **Prefect UI** *(optional)* — `prefect server start`, rerun `python main.py`, open `http://127.0.0.1:4200`.

---

## Prefect UI (optional)

```bash
prefect server start
```

Open `http://127.0.0.1:4200` to inspect runs triggered via `main.py`.

---

## pgAdmin / host Postgres

For **host-installed** Postgres or Cockroach, set **`DATABASE_URL`** in **`.env`** to match your instance. GUI tools against managed DBs: prefer **psql** or a vendor-compatible client if pgAdmin misbehaves.

---

## AI usage declaration (historical)

Tool: Gemini  
Used for: Early Docker/port debugging, schema and Prefect scaffolding  
Extent: Boilerplate and debugging assistance
