--- by weekday,
--- count of total trips, trips starting and ending in a different borough, and
--- percentage w/ different start/end

WITH 
taxi_trips AS (
    SELECT
        weekday(pickup_datetime) as weekday,
        CAST(pulocationid AS VARCHAR) AS start_station_id,
        CAST(dolocationid AS VARCHAR) AS end_station_id
    FROM {{ ref('mart__fact_all_taxi_trips') }}
),
trip_locations AS (
    SELECT
        t.weekday,
        s.borough AS start_borough,
        e.borough AS end_borough
    FROM taxi_trips t
    JOIN {{ ref('mart__dim_locations') }} s ON t.start_station_id = CAST(s.locationid AS VARCHAR)
    JOIN {{ ref('mart__dim_locations') }} e ON t.end_station_id = CAST(e.locationid AS VARCHAR)
),

aggregated_data AS (
    SELECT
        weekday,
        COUNT(*) AS total_trips,
        COUNT(*) FILTER (WHERE start_borough <> end_borough) AS diff_borough_trips
    FROM trip_locations
    GROUP BY weekday
)

SELECT
    weekday,
    total_trips,
    diff_borough_trips,
    ROUND((diff_borough_trips::FLOAT / total_trips) * 100, 2) AS percent_diff_start_end
FROM aggregated_data
ORDER BY weekday

--- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/inter_borough_taxi_trips_by_weekday.sql" > answers/inter_borough_trips.txt