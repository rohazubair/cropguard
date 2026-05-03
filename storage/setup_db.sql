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

CREATE TABLE IF NOT EXISTS gold.v_dashboard_forecast_weather (
    run_id BIGINT NOT NULL,
    district_id BIGINT NOT NULL,
    district_name VARCHAR(50) NOT NULL,
    lat DECIMAL(9,6) NOT NULL,
    lon DECIMAL(9,6) NOT NULL,
    valid_date DATE NOT NULL,
    horizon_day SMALLINT NOT NULL,
    temp_p10 DOUBLE PRECISION,
    temp_p50 DOUBLE PRECISION,
    temp_p90 DOUBLE PRECISION,
    humidity_p10 DOUBLE PRECISION,
    humidity_p50 DOUBLE PRECISION,
    humidity_p90 DOUBLE PRECISION,
    run_started_at TIMESTAMP,
    run_finished_at TIMESTAMP,
    input_weather_until TIMESTAMP,
    run_status VARCHAR(24),
    PRIMARY KEY (run_id, district_id, valid_date)
);

CREATE TABLE IF NOT EXISTS gold.v_dashboard_forecast_crop_risk (
    run_id BIGINT NOT NULL,
    district_id BIGINT NOT NULL,
    district_name VARCHAR(50) NOT NULL,
    crop_name VARCHAR(128) NOT NULL,
    valid_date DATE NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    risk_tier VARCHAR(24) NOT NULL,
    drivers_json JSONB,
    input_weather_until TIMESTAMP,
    run_status VARCHAR(24),
    PRIMARY KEY (run_id, district_id, crop_name, valid_date)
);

CREATE TABLE IF NOT EXISTS gold.v_dashboard_published_forecast_run (
    run_id BIGINT PRIMARY KEY,
    status VARCHAR(24) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    input_weather_until TIMESTAMP,
    horizon_days INT NOT NULL,
    granularity VARCHAR(24) NOT NULL,
    drift_checked BOOLEAN,
    drift_retrain_triggered BOOLEAN,
    model_id BIGINT,
    model_name VARCHAR(64),
    model_version VARCHAR(48),
    artifact_uri TEXT,
    model_trained_at TIMESTAMP,
    published_at TIMESTAMP NOT NULL
);
