
CREATE TABLE DimDistrict (
    district_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE,
    province VARCHAR(50),
    lat DECIMAL(9,6),
    lon DECIMAL(9,6)
);

CREATE TABLE FactWeatherReadings (
    reading_id SERIAL PRIMARY KEY,
    district_id INT REFERENCES DimDistrict(district_id),
    timestamp TIMESTAMP,
    temp_min DECIMAL(5,2),
    temp_max DECIMAL(5,2),
    precipitation_mm DECIMAL(5,2),
    humidity_pct INT,
    soil_moisture DECIMAL(5,2),
    UNIQUE(district_id, timestamp) 
);

INSERT INTO DimDistrict (name, province, lat, lon) VALUES 
('Lahore', 'Punjab', 31.5204, 74.3587),
('Sahiwal', 'Punjab', 30.6682, 73.1114);