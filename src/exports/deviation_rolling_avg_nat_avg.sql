
-- This is the deviation from the rolling avg temperature compared to the national average
-- This should eliminate more of the noise
WITH nationaal_daggemiddelde AS (
    SELECT
        d.date,
        AVG(f.temp_avg) AS nationaal_avg
    FROM fact_weather f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.date
),
dagelijkse_afwijking AS (
    SELECT
        s.name,
        f.station_id,
        d.date,
        ROUND(f.temp_avg - n.nationaal_avg, 2) AS afwijking
    FROM fact_weather f
    JOIN dim_station s ON f.station_id = s.station_id
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN nationaal_daggemiddelde n ON d.date = n.date
)
SELECT
    name,
    date,
    afwijking,
    ROUND(AVG(afwijking) OVER (
        PARTITION BY station_id
        ORDER BY date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_afwijking_90d
FROM dagelijkse_afwijking
ORDER BY name, date