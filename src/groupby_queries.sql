-- ============================================
-- Level 2: GROUP BY + HAVING
-- ============================================

-- 1. Jaren met meer dan 20 hittedagen (TX > 25°C) per station
SELECT
    s.name,
    d.year,
    COUNT(*) AS hittedagen
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.temp_max > 25
GROUP BY s.name, d.year
HAVING COUNT(*) > 20
ORDER BY hittedagen DESC;

-- 2. Stations met meer neerslag dan het landelijk gemiddelde
SELECT
    s.name,
    ROUND(AVG(f.precipitation), 2) AS avg_daily_precip,
    ROUND((SELECT AVG(precipitation) FROM fact_weather), 2) AS national_avg
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
GROUP BY s.name
HAVING AVG(f.precipitation) > (SELECT AVG(precipitation) FROM fact_weather)
ORDER BY avg_daily_precip DESC;

-- 3. Maanden met gemiddeld meer dan 6 uur zon per dag per station
SELECT
    s.name,
    d.month_name,
    ROUND(AVG(f.sunshine_hrs), 1) AS avg_sunshine
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.name, d.month_name, d.month
HAVING AVG(f.sunshine_hrs) > 6
ORDER BY avg_sunshine DESC;

-- 4. Gemiddelde neerslag per seizoen, alleen seizoenen natter dan 2mm/dag
SELECT
    s.name,
    d.season,
    ROUND(AVG(f.precipitation), 2) AS avg_precip
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.name, d.season
HAVING AVG(f.precipitation) > 2
ORDER BY s.name, avg_precip DESC;