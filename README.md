# KNMI Weather Data Exploration

With this project I wanna further solidify my SQL knowledge by querying KNMI weather data and get some nice insights on weather patterns over the period 2000-2026.

## Database

I used PostgreSQL (version 18.3) for the database design.

The following tables were created:

- fact_weather
- dim_date
- dim_station

So I essentially went for a Star Schema, as it allows for better and more interesting queries. 

For this project I decided to only pick the 5 most important weather stations that, together, cover all regions of the netherlands:

- De Bilt
- Schiphol Airport
- Groningen Airport Eelde
- Vlissingen
- Eindhoven Airport

Quick summary of the data:

| Station Name            | Entries | Start Date | End Date   |
|-------------------------|---------|------------|------------|
| De Bilt                 | 9497    | 2000-04-01 | 2026-04-01 |
| Eindhoven Airport       | 9497    | 2000-04-01 | 2026-04-01 |
| Groningen Airport Eelde | 9497    | 2000-04-01 | 2026-04-01 |
| Schiphol Airport        | 9497    | 2000-04-01 | 2026-04-01 |
| Vlissingen              | 9497    | 2000-04-01 | 2026-04-01 |

## Limitations

Because I couldnt download all weather data from all stations located in the Netherlands (too many results - had to cut the parameters), I decided to go for the 5 weather stations to cover all regions. 

For a more thorough analysis, it is recommended to obtain more data from different weather stations.