--- seven_day_moving_aggs_weather.sql
--- query to calculate the 7 day moving min, max, avg, sum for precipitation and snow for every day in the weather data

SELECT 
    date,
    MIN(prcp) OVER seven_days AS min_precipitation,
    MAX(prcp) OVER seven_days AS max_precipitation,
    AVG(prcp) OVER seven_days AS avg_precipitation,
    SUM(prcp) OVER seven_days AS sum_precipitation,
    MIN(snow) OVER seven_days AS min_snow,
    MAX(snow) OVER seven_days AS max_snow,
    AVG(snow) OVER seven_days AS avg_snow,
    SUM(snow) OVER seven_days AS sum_snow
FROM {{ ref('stg__central_park_weather')}}
WINDOW seven_days AS (
    ORDER BY date ASC 
    ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
)

--- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/seven_day_moving_aggs_weather.sql" > answers/seven_day_moving_aggs_weather.txt