-- ============================================
-- KNMI Weather Data - Star Schema
-- ============================================

-- Dimension: Station
-- Compared to the original dataset, I have added:
-- region: to group stations by geographic area (e.g., North, South, East, West)
-- coast_type: to classify stations as 'Coastal' or 'Inland' based on their proximity to the sea, which can influence weather patterns
CREATE TABLE IF NOT EXISTS dim_station (
    station_id      INTEGER PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    longitude       NUMERIC(6,3) NOT NULL,
    latitude        NUMERIC(6,3) NOT NULL,
    altitude_m      NUMERIC(6,2) NOT NULL,
    region          VARCHAR(100) NOT NULL,
    coast_type      VARCHAR(20) NOT NULL 
);

-- Dimension: Date
-- I have added several attributes to the date dimension to enable more detailed time-based analysis
CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    date            DATE NOT NULL UNIQUE,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    quarter         INTEGER NOT NULL,
    season          VARCHAR(20) NOT NULL,  
    weekday         INTEGER NOT NULL,     
    weekday_name    VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

-- Fact Table: Daily Weather Measurements
-- The fact table contains the core weather measurements for each station and date.
CREATE TABLE IF NOT EXISTS fact_weather (
    id              SERIAL PRIMARY KEY,
    station_id      INTEGER NOT NULL REFERENCES dim_station(station_id),
    date_id         INTEGER NOT NULL REFERENCES dim_date(date_id),
    wind_speed_avg  NUMERIC(5,1),   -- m/s 
    temp_avg        NUMERIC(4,1),   -- °C 
    temp_min        NUMERIC(4,1),   -- °C 
    temp_max        NUMERIC(4,1),   -- °C 
    sunshine_hrs    NUMERIC(4,1),   -- uur 
    sunshine_pct    INTEGER,        -- percentage
    precipitation   NUMERIC(5,1),   -- mm 
    UNIQUE(station_id, date_id),
    -- Sanity checks
    CONSTRAINT chk_temp CHECK (temp_avg BETWEEN -40 AND 50),
    CONSTRAINT chk_precip CHECK (precipitation >= 0),
    CONSTRAINT chk_sunshine_pct CHECK (sunshine_pct BETWEEN 0 AND 100)
);