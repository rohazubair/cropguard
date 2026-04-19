CREATE SCHEMA IF NOT EXISTS cropguard_dev;

CREATE TABLE IF NOT EXISTS cropguard_dev.DimDistrict (
    district_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE,
    province VARCHAR(50),
    lat DECIMAL(9,6),
    lon DECIMAL(9,6)
);

CREATE TABLE IF NOT EXISTS cropguard_dev.FactWeatherReadings (
    reading_id SERIAL PRIMARY KEY,
    district_id INT REFERENCES cropguard_dev.DimDistrict(district_id),
    timestamp TIMESTAMP,
    temp_min DECIMAL(5,2),
    temp_max DECIMAL(5,2),
    precipitation_mm DECIMAL(5,2),
    humidity_pct INT,
    soil_moisture DECIMAL(5,2),
    UNIQUE(district_id, timestamp)
);

INSERT INTO cropguard_dev.DimDistrict (name, province, lat, lon) VALUES
('Lahore', 'Punjab', 31.520400, 74.358700),
('Karachi', 'Sindh', 24.860700, 67.001100),
('Islamabad', 'Islamabad Capital Territory', 33.684400, 73.047900),
('Faisalabad', 'Punjab', 31.450400, 73.087900),
('Rawalpindi', 'Punjab', 33.600700, 73.067900),
('Multan', 'Punjab', 30.198400, 71.468700),
('Peshawar', 'Khyber Pakhtunkhwa', 34.015100, 71.575000),
('Quetta', 'Balochistan', 30.179800, 66.975000),
('Gujranwala', 'Punjab', 32.161700, 74.188300),
('Hyderabad', 'Sindh', 25.396400, 68.377800),
('Sialkot', 'Punjab', 32.492500, 74.531300),
('Bahawalpur', 'Punjab', 29.395600, 71.672200),
('Sargodha', 'Punjab', 32.085400, 72.675000),
('Sukkur', 'Sindh', 27.713900, 68.836900),
('Abbottabad', 'Khyber Pakhtunkhwa', 34.143900, 73.211400)
ON CONFLICT (name) DO NOTHING;
