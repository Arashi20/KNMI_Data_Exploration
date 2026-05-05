-- ============================================
-- Level 3: Window Functions
-- Hier maken we gebruik van OVER......PARTITION BY...
--- Dit zorgt ervoor dat je 1 getal krijgt PER RIJ, en niet voor de gehele groep samen.
-- ============================================

-- 1. 30-daags voortschrijdend gemiddelde temperatuur per station (goed voor analyse in Power BI)
SELECT
    s.name,
    d.date,
    f.temp_avg,
    ROUND(AVG(f.temp_avg) OVER (
        PARTITION BY f.station_id
        ORDER BY d.date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 1) AS rolling_avg_30d   -- Dit is een meebewegende gemiddelde van de afgelopen 30 dagen (of minder als je nog in het begin van de data zit)
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
ORDER BY s.name, d.date;

-- 2. Jaar-op-jaar temperatuurverschil per station met LAG
SELECT
    s.name,
    d.year,
    ROUND(AVG(f.temp_avg), 1) AS avg_temp,
    ROUND(AVG(f.temp_avg) - LAG(ROUND(AVG(f.temp_avg), 1)) OVER (
        PARTITION BY s.name
        ORDER BY d.year
    ), 1) AS diff_vs_prev_year
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.name, f.station_id, d.year
ORDER BY s.name, d.year;

-- 3. Ranking warmste zomers per station
SELECT
    s.name,
    d.year,
    ROUND(AVG(f.temp_avg), 1) AS avg_zomer_temp,
    RANK() OVER (
        PARTITION BY s.name
        ORDER BY AVG(f.temp_avg) DESC
    ) AS rank_warmste_zomer
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.season = 'Zomer'
GROUP BY s.name, f.station_id, d.year
ORDER BY s.name, rank_warmste_zomer;

-- 4. Cumulatieve neerslag per jaar per station (goed voor analyse in Power BI)
SELECT
    s.name,
    d.date,
    d.year,
    f.precipitation,
    ROUND(SUM(f.precipitation) OVER (
        PARTITION BY f.station_id, d.year
        ORDER BY d.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 1) AS cumulative_precip_ytd
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
ORDER BY s.name, d.date;