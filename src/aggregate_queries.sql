-- ============================================
-- KNMI Weather Queries
-- Level 1: Basis Aggregaties
-- ============================================


-- Snelle fixes hieronder toevoegen:



-- 1. Gemiddelde temperatuur per jaar per station
SELECT 
    s.name,  -- Dit betekent dus gewoon: pak de kolom "name" uit de tabel met alias s. De alias is in the JOIN statement gedefinieerd.
    d.year,  -- SQL queries lezen niet hetzelfde als Python code, wat voor mij een mindshift was.
    ROUND(AVG(f.temp_avg), 1) AS avg_temp
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id -- linkt de feitentabel door middel van de foreign key naar de juiste stations
JOIN dim_date d ON f.date_id = d.date_id -- linkt de feitentabel door middel van de foreign key naar de juiste datum
GROUP BY s.name, d.year
HAVING COUNT(*) > 350  -- filter 2026 eruit, aangezien de gemiddelde daarvan veel lager is uiteraard (jan-apr).
ORDER BY s.name, d.year;



-- 2. Top 10 natste maanden
SELECT 
    s.name,
    d.year,
    d.month_name,
    ROUND(SUM(f.precipitation), 1) AS total_precipitation_mm
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.name, d.year, d.month_name, d.month
ORDER BY total_precipitation_mm DESC
LIMIT 10;

-- 3. Gemiddelde zonneschijn per seizoen per station
SELECT
    s.name,
    d.season,
    ROUND(AVG(f.sunshine_hrs), 1) AS avg_sunshine_hrs
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.name, d.season
ORDER BY s.name, avg_sunshine_hrs DESC;

-- 4. Koudste en warmste dag ooit per station
SELECT
    s.name,
    MIN(f.temp_min) AS coldest_temp,
    MAX(f.temp_max) AS hottest_temp
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
GROUP BY s.name
ORDER BY s.name;

-- 5. Gemiddelde windsnelheid per regio (kust vs inland)
SELECT
    s.coast_type,
    ROUND(AVG(f.wind_speed_avg), 1) AS avg_wind_speed
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
GROUP BY s.coast_type
ORDER BY avg_wind_speed DESC;



-- Test queries hieronder:


