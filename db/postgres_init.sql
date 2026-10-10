-- Crime Trends Table
CREATE TABLE IF NOT EXISTS crime_trends (
    id SERIAL PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    day_of_week VARCHAR(20),
    hour INTEGER,
    crime_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Arrest Rates Table
CREATE TABLE IF NOT EXISTS arrest_rates (
    id SERIAL PRIMARY KEY,
    primary_type VARCHAR(100),
    district VARCHAR(10),
    race VARCHAR(50),
    total_crimes INTEGER,
    total_arrests INTEGER,
    arrest_rate FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Hotspots Table (K-Means clusters)
CREATE TABLE IF NOT EXISTS hotspots (
    id SERIAL PRIMARY KEY,
    cluster_id INTEGER,
    centroid_lat FLOAT,
    centroid_lon FLOAT,
    crime_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alerts Table (from Storm)
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    district VARCHAR(10),
    event_count INTEGER,
    threshold INTEGER,
    severity VARCHAR(20),
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Correlations Table
CREATE TABLE IF NOT EXISTS correlations (
    id SERIAL PRIMARY KEY,
    correlation_type VARCHAR(100),
    district VARCHAR(10),
    community_area VARCHAR(100),
    violence_rate FLOAT,
    arrest_rate FLOAT,
    sex_offender_density FLOAT,
    crime_rate FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Violence Stats Table
CREATE TABLE IF NOT EXISTS violence_stats (
    id SERIAL PRIMARY KEY,
    month INTEGER,
    district VARCHAR(10),
    community_area VARCHAR(100),
    total_homicides INTEGER,
    non_fatal_shootings INTEGER,
    gunshot_injuries INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sex Offender Stats Table
CREATE TABLE IF NOT EXISTS sex_offender_stats (
    id SERIAL PRIMARY KEY,
    district VARCHAR(10),
    offender_count INTEGER,
    victim_minor_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
