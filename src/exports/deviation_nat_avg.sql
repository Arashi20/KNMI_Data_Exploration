WITH nationaal_daggemiddelde AS (
    SELECT
        d.date,
        AVG(f.temp_avg) AS nationaal_avg
    FROM fact_weather f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.date
)
SELECT
    s.name,
    d.date,
    f.temp_avg,
    ROUND(f.temp_avg - n.nationaal_avg, 2) AS afwijking_van_nationaal
FROM fact_weather f
JOIN dim_station s ON f.station_id = s.station_id
JOIN dim_date d ON f.date_id = d.date_id
JOIN nationaal_daggemiddelde n ON d.date = n.date
ORDER BY s.name, d.date