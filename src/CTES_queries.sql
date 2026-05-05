-- ============================================
-- Level 4: CTEs (Common Table Expressions)
-- met WITH zorg je ervoor dat je een tijdelijke tabekl definieert waardoor de gehele query makkelijker te lezen valt
-- ============================================

-- 1. Top 3 warmste zomers per station
WITH zomer_gemiddelden AS (
    SELECT
        s.name,
        d.year,
        ROUND(AVG(f.temp_avg), 1) AS avg_zomer_temp
    FROM fact_weather f
    JOIN dim_station s ON f.station_id = s.station_id
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.season = 'Zomer'
    GROUP BY s.name, f.station_id, d.year
),
gerankt AS (
    SELECT
        name,
        year,
        avg_zomer_temp,
        RANK() OVER (PARTITION BY name ORDER BY avg_zomer_temp DESC) AS rank
    FROM zomer_gemiddelden
)
SELECT * FROM gerankt
WHERE rank <= 3
ORDER BY name, rank;

-- 2. Droogste jaren per station vs nationaal gemiddelde
WITH jaarlijkse_neerslag AS (
    SELECT
        s.name,
        d.year,
        ROUND(SUM(f.precipitation), 1) AS total_neerslag
    FROM fact_weather f
    JOIN dim_station s ON f.station_id = s.station_id
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY s.name, f.station_id, d.year
),
nationaal_gemiddelde AS (
    SELECT ROUND(AVG(total_neerslag), 1) AS nationaal_avg
    FROM jaarlijkse_neerslag
)
SELECT
    j.name,
    j.year,
    j.total_neerslag,
    n.nationaal_avg,
    ROUND(j.total_neerslag - n.nationaal_avg, 1) AS verschil
FROM jaarlijkse_neerslag j
CROSS JOIN nationaal_gemiddelde n
WHERE j.total_neerslag < n.nationaal_avg
AND j.year < 2026 --- incompleet jaar
ORDER BY j.total_neerslag ASC
LIMIT 15;

-- 3. Hittegolf detectie (5+ opeenvolgende dagen boven 25 graden)
WITH dagelijkse_hits AS (
    SELECT
        s.name,
        d.date,
        f.temp_max,
        CASE WHEN f.temp_max > 25 THEN 1 ELSE 0 END AS is_heet
    FROM fact_weather f
    JOIN dim_station s ON f.station_id = s.station_id
    JOIN dim_date d ON f.date_id = d.date_id
),
groepen AS (
    SELECT
        name,
        date,
        temp_max,
        is_heet,
        date - (ROW_NUMBER() OVER (
            PARTITION BY name, is_heet
            ORDER BY date
        ) * INTERVAL '1 day') AS groep_id
    FROM dagelijkse_hits
),
hittegolven AS (
    SELECT
        name,
        MIN(date) AS start_datum,
        MAX(date) AS eind_datum,
        COUNT(*) AS aantal_dagen,
        ROUND(AVG(temp_max), 1) AS gem_max_temp
    FROM groepen
    WHERE is_heet = 1
    GROUP BY name, groep_id
    HAVING COUNT(*) >= 5
)
SELECT *
FROM hittegolven
ORDER BY aantal_dagen DESC;

-- 4. Meest extreme weersdag per station per jaar
WITH dagelijkse_extremen AS (
    SELECT
        s.name,
        d.year,
        d.date,
        f.temp_max,
        f.precipitation,
        f.wind_speed_avg,
        -- Combine extremen in één score
        ROUND(
            (f.temp_max / 10.0) +
            (f.precipitation / 5.0) +
            (f.wind_speed_avg / 3.0)
        , 1) AS extreme_score
    FROM fact_weather f
    JOIN dim_station s ON f.station_id = s.station_id
    JOIN dim_date d ON f.date_id = d.date_id
),
gerankt AS (
    SELECT *,
        RANK() OVER (
            PARTITION BY name, year
            ORDER BY extreme_score DESC
        ) AS rank
    FROM dagelijkse_extremen
)
SELECT
    name,
    year,
    date,
    temp_max,
    precipitation,
    wind_speed_avg,
    extreme_score
FROM gerankt
WHERE rank = 1
ORDER BY extreme_score DESC
LIMIT 20;