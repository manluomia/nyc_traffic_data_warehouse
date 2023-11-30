---Write a query to calculate the 7 day moving average precipitation for every day in
--- the weather data.

SELECT date, 
AVG(prcp) OVER (
        ORDER BY date 
        ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
    ) AS moving_avg_precipitation
FROM {{ ref('stg__central_park_weather')}}

-- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/seven_day_moving_average_prcp.sql" > answers/seven_day_moving_average_prcp.txt