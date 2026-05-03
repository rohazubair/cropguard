CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.districts_dim (
    district_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE,
    lat DECIMAL(9,6),
    lon DECIMAL(9,6)
);


CREATE TABLE IF NOT EXISTS bronze.weather_readings_fact (
    reading_id SERIAL PRIMARY KEY,
    district_id BIGINT NOT NULL REFERENCES bronze.districts_dim(district_id),
    observed_ts TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    humidity_pct INT NOT NULL,
    UNIQUE (district_id, observed_ts)
);


CREATE TABLE IF NOT EXISTS bronze.crop_facts (
    id SERIAL PRIMARY KEY,
    n SMALLINT NOT NULL,
    p SMALLINT NOT NULL,
    k SMALLINT NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    humidity DOUBLE PRECISION NOT NULL,
    ph DOUBLE PRECISION NOT NULL,
    rainfall DOUBLE PRECISION NOT NULL,
    label VARCHAR(64) NOT NULL,
    district VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.ingestion_state (
    key VARCHAR(128) PRIMARY KEY,
    content_hash TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.districts_dim AS
SELECT district_id, name, lat, lon
FROM bronze.districts_dim;

CREATE TABLE IF NOT EXISTS silver.crop_facts (
    id BIGSERIAL PRIMARY KEY,
    district_id BIGINT NOT NULL REFERENCES bronze.districts_dim(district_id),
    temperature DOUBLE PRECISION NOT NULL,
    humidity DOUBLE PRECISION NOT NULL,
    crop_name VARCHAR(128) NOT NULL,
    district VARCHAR(80) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.weather_readings_fact (
    reading_id SERIAL PRIMARY KEY,
    district_id BIGINT NOT NULL REFERENCES bronze.districts_dim(district_id),
    observed_ts TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    humidity_pct INT NOT NULL,
    UNIQUE (district_id, observed_ts)
);


CREATE TABLE IF NOT EXISTS silver.ml_model_registry (
    model_id SERIAL PRIMARY KEY,
    model_name VARCHAR(64) NOT NULL,
    version VARCHAR(48) NOT NULL,
    artifact_uri TEXT NOT NULL,
    trained_at TIMESTAMP NOT NULL DEFAULT now(),
    train_data_end_ts TIMESTAMP,
    metrics_json JSONB,
    feature_reference_json JSONB,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (model_name, version)
);

CREATE TABLE IF NOT EXISTS silver.ml_forecast_run (
    run_id BIGSERIAL PRIMARY KEY,
    model_id INT REFERENCES silver.ml_model_registry(model_id),
    started_at TIMESTAMP NOT NULL DEFAULT now(),
    finished_at TIMESTAMP,
    status VARCHAR(24) NOT NULL DEFAULT 'running',
    input_weather_until TIMESTAMP,
    horizon_days INT NOT NULL DEFAULT 7,
    granularity VARCHAR(24) NOT NULL DEFAULT 'daily',
    drift_checked BOOLEAN DEFAULT FALSE,
    drift_retrain_triggered BOOLEAN DEFAULT FALSE,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS silver.ml_forecast_run_published (
    key VARCHAR(32) PRIMARY KEY,
    run_id BIGINT REFERENCES silver.ml_forecast_run(run_id),
    published_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.ml_forecast_weather_point (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES silver.ml_forecast_run(run_id),
    district_id BIGINT NOT NULL REFERENCES bronze.districts_dim(district_id),
    valid_date DATE NOT NULL,
    horizon_day SMALLINT NOT NULL,
    temp_p10 DOUBLE PRECISION,
    temp_p50 DOUBLE PRECISION,
    temp_p90 DOUBLE PRECISION,
    humidity_p10 DOUBLE PRECISION,
    humidity_p50 DOUBLE PRECISION,
    humidity_p90 DOUBLE PRECISION,
    UNIQUE (run_id, district_id, valid_date)
);

CREATE TABLE IF NOT EXISTS silver.ml_forecast_crop_risk_point (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES silver.ml_forecast_run(run_id),
    district_id BIGINT NOT NULL REFERENCES bronze.districts_dim(district_id),
    crop_name VARCHAR(128) NOT NULL,
    valid_date DATE NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    risk_tier VARCHAR(24) NOT NULL,
    drivers_json JSONB,
    UNIQUE (run_id, district_id, crop_name, valid_date)
);

CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE VIEW gold.v_dashboard_forecast_weather AS
SELECT
    w.run_id,
    w.district_id,
    d.name AS district_name,
    d.lat,
    d.lon,
    w.valid_date,
    w.horizon_day,
    w.temp_p10,
    w.temp_p50,
    w.temp_p90,
    w.humidity_p10,
    w.humidity_p50,
    w.humidity_p90,
    r.started_at AS run_started_at,
    r.finished_at AS run_finished_at,
    r.input_weather_until,
    r.status AS run_status
FROM silver.ml_forecast_weather_point w
JOIN silver.ml_forecast_run_published p ON p.key = 'default' AND w.run_id = p.run_id
JOIN silver.districts_dim d ON d.district_id = w.district_id
LEFT JOIN silver.ml_forecast_run r ON r.run_id = w.run_id;

CREATE OR REPLACE VIEW gold.v_dashboard_forecast_crop_risk AS
SELECT
    c.run_id,
    c.district_id,
    d.name AS district_name,
    c.crop_name,
    c.valid_date,
    c.risk_score,
    c.risk_tier,
    c.drivers_json,
    r.input_weather_until,
    r.status AS run_status
FROM silver.ml_forecast_crop_risk_point c
JOIN silver.ml_forecast_run_published p ON p.key = 'default' AND c.run_id = p.run_id
JOIN silver.districts_dim d ON d.district_id = c.district_id
LEFT JOIN silver.ml_forecast_run r ON r.run_id = c.run_id;

CREATE OR REPLACE VIEW gold.v_dashboard_published_forecast_run AS
SELECT
    r.run_id,
    r.status,
    r.started_at,
    r.finished_at,
    r.input_weather_until,
    r.horizon_days,
    r.granularity,
    r.drift_checked,
    r.drift_retrain_triggered,
    m.model_id,
    m.model_name,
    m.version AS model_version,
    m.artifact_uri,
    m.trained_at AS model_trained_at,
    p.published_at
FROM silver.ml_forecast_run_published p
JOIN silver.ml_forecast_run r ON r.run_id = p.run_id AND p.key = 'default'
LEFT JOIN silver.ml_model_registry m ON m.model_id = r.model_id;
