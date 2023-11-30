--- Write a query to determine if days immediately preceding precipitation or snow
--- had more bike trips on average than the days with precipitation or snow

WITH weather AS (
    -- Selecting weather data and determining if there was precipitation or snow
    SELECT 
        date as weather_date, -- Date of weather observation
        CASE 
            WHEN prcp > 0 OR snow > 0 THEN 'Yes' -- Mark as 'Yes' if there's any precipitation or snow
            ELSE 'No' -- Otherwise, mark as 'No'
        END as PrecipOrSnow
    FROM {{ref('stg__central_park_weather')}} -- Reference to the staging weather table
),
trips AS (
    -- Converting the timestamp to a date for each trip
    SELECT started_at_ts::date as trip_date
    FROM {{ref('mart__fact_all_bike_trips')}} -- Reference to the bike trips table
),
combined_trips as (
    -- Counting the number of trips per day
    SELECT trip_date, count(*) as number_of_trips
    FROM trips
    GROUP BY trip_date -- Grouping by trip date for aggregation
),
trips_with_weather AS (
    -- Joining the trip counts with weather data
    SELECT 
        b.*,
        w.PrecipOrSnow -- Adding weather condition (precipitation/snow) to each trip date
    FROM combined_trips b
    LEFT JOIN weather w ON b.trip_date = DATE_ADD(w.weather_date, INTERVAL 1 DAY) -- Joining on the condition that the trip date is one day after the weather observation
)
-- Final selection
SELECT 
    PrecipOrSnow,
    AVG(number_of_trips) as AverageTrips -- Calculating average number of trips for days with and without precipitation/snow
FROM trips_with_weather
WHERE PrecipOrSnow is not null -- Filtering out records where weather data is missing
GROUP BY PrecipOrSnow -- Grouping by weather condition to get separate averages



--- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/days_before_precip_more_bike_trips.sql" > answers/days_before_precip_more_bike_trips.txt