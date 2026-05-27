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