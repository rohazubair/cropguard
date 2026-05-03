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
| `docker-compose.yml` | **Prefect server** (UI **4200**), FastAPI, Streamlit; **`env_file: .env`** on app services; **`pipeline`** profile + **`PREFECT_API_URL`** to the compose Prefect server; **`cropguard_artifacts`** and **`prefect_home`** volumes. |

---

## Run with Docker

From **`cropguard/`** with [Docker Compose](https://docs.docker.com/compose/) installed.

**Same database as on the host:** put **`DATABASE_URL`** in **`.env`** (same file you use for `python main.py`). Compose injects that file into **`api`**, **`streamlit`**, and **`pipeline`** via **`env_file: .env`** (the **Prefect** server service does not load **`.env`** so a client **`PREFECT_API_URL`** there cannot confuse the server). There is **no bundled Postgres**—containers use your URL (e.g. Cockroach Cloud) like a local run.

Copy **`.env.example`** → **`.env`** if you do not have one yet, then set **`DATABASE_URL`**.

**Ports:** Prefect UI **4200**, API **8000**, Streamlit **8501**. Streamlit always calls the API at **`http://api:8000`** inside Docker (compose **`environment`** overrides any host-only **`CROPGUARD_API_BASE`** in **`.env`** for that service).

**Prefect:** The **`prefect`** service runs **`prefect server start --host 0.0.0.0 --port 4200`**. The **`pipeline`** service sets **`PREFECT_API_URL=http://prefect:4200/api`** so flow runs show up in that UI (this overrides any **`PREFECT_API_URL`** in **`.env`** for pipeline only). Open **`http://127.0.0.1:4200`** on your machine while compose is up. Do not run a second Prefect server on the host on port **4200** at the same time.

**TLS:** The image installs **`ca-certificates`** so **`sslmode=verify-full`** against managed providers usually works. If your provider requires a custom CA, add **`sslrootcert=...`** to **`DATABASE_URL`** (path must be valid *inside* the container, e.g. mount a file under **`/app/certs`** and reference it).

1. **`.env`** with **`DATABASE_URL`** set (required).

2. **Build and start Prefect + API + Streamlit**

```bash
docker compose build
docker compose up -d
```

Wait until **`cropguard_prefect`** is healthy (first boot can take ~30s). Then open **`http://127.0.0.1:4200`** for the Prefect UI.

3. **Run the full Prefect pipeline once** (ingest + forecast; needs outbound internet for Open-Meteo and geocoding). Compose starts the **`prefect`** service first if it is not running, then waits for it to be healthy. Trained model artifacts are stored in the **`cropguard_artifacts`** volume.

```bash
docker compose --profile pipeline run --rm pipeline
```

4. **Forecast only** (after data exists)

```bash
docker compose --profile pipeline run --rm pipeline python main.py --forecast-only
```

5. **Open in the browser**

- Prefect UI: `http://127.0.0.1:4200`
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

Add **`-v`** only if you want to remove the **ML artifact** volume **`cropguard_artifacts`** and the **Prefect metadata** volume **`prefect_home`** (your cloud database is unchanged).

---

## Docker commands (reference)

Run these from **`cropguard/`** (where **`docker-compose.yml`** is).

### Status

```bash
docker compose ps
```

### URLs (after `docker compose up -d`)

| App | URL |
|-----|-----|
| Prefect UI | `http://127.0.0.1:4200` |
| FastAPI (Swagger) | `http://127.0.0.1:8000/docs` |
| FastAPI health | `http://127.0.0.1:8000/healthz` |
| Streamlit | `http://127.0.0.1:8501` |
| pgAdmin *(if started with `--profile tools`)* | `http://127.0.0.1:8080` |

### Logs (running stack)

Follow logs from all default services:

```bash
docker compose logs -f
```

One service (service names from compose: **`prefect`**, **`api`**, **`streamlit`**):

```bash
docker compose logs -f api
docker compose logs -f streamlit
docker compose logs -f prefect
```

Last lines only, then follow:

```bash
docker compose logs -f --tail=200 prefect
```

Print logs once (no follow):

```bash
docker compose logs api
```

Same thing by **container name**:

```bash
docker logs -f cropguard_api
docker logs -f cropguard_streamlit
docker logs -f cropguard_prefect
```

**Pipeline** (`docker compose --profile pipeline run --rm pipeline`): output goes to **your terminal** for that command. There is no long-running **`pipeline`** container to **`docker compose logs`** after it exits (unless you drop **`--rm`**).

### Stop containers

```bash
docker compose down
```

If you started **pgAdmin** with the **`tools`** profile, stop it too:

```bash
docker compose --profile tools down
```

Remove containers **and** named volumes (**`cropguard_artifacts`**, **`prefect_home`**; not your cloud DB):

```bash
docker compose down -v
```

### Run pipeline before bringing up the UI

You may run the full pipeline first, then start the web stack (same **`.env`**):

```bash
docker compose build
docker compose --profile pipeline run --rm pipeline
docker compose up -d
```

### Docker Desktop

To stop **all** containers on your machine, quit **Docker Desktop** from the system tray / menu (Windows or Mac).

---

## Environment

Create **`cropguard/.env`** (see **`.env.example`**). At minimum:

```env
DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DATABASE?sslmode=require
```

**Docker:** **`api`**, **`streamlit`**, and **`pipeline`** load **`.env`**. **`pipeline`** also receives **`PREFECT_API_URL=http://prefect:4200/api`** from compose so runs register with the in-stack Prefect server (same DB as host; Prefect API target is Docker-specific).

Optional for **host** Streamlit when the API is not on port 8000:

```env
CROPGUARD_API_BASE=http://127.0.0.1:8000
```

---

## Quick start

```bash
cd cropguard
python -m venv .venv
pip install -r requirements.txt
```

Apply DDL (or rely on `python main.py`, which runs `apply_bronze_schema` first).

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

**Docker:** use the **`prefect`** service from **`docker compose up`** and open **`http://127.0.0.1:4200`** (see **Run with Docker**).

**Host only** (no Docker Prefect): start a server locally, then run **`main.py`** so the client can reach it:

```bash
prefect server start
```

Set **`PREFECT_API_URL=http://127.0.0.1:4200/api`** in **`.env`** (or your shell) so host runs are recorded there.

**Host + Docker Prefect:** with **`docker compose up`** exposing **4200**, you can point host runs at the same server using **`PREFECT_API_URL=http://127.0.0.1:4200/api`** in **`.env`**. Do not start a second **`prefect server start`** on the host on the same port.

---

## pgAdmin / host Postgres

For **host-installed** Postgres or Cockroach, set **`DATABASE_URL`** in **`.env`** to match your instance. GUI tools against managed DBs: prefer **psql** or a vendor-compatible client if pgAdmin misbehaves.

---

## AI usage declaration (historical)

Tool: Gemini  
Used for: Early Docker/port debugging, schema and Prefect scaffolding  
Extent: Boilerplate and debugging assistance
