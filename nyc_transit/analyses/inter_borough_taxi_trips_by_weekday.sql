--- by weekday,
--- count of total trips, trips starting and ending in a different borough, and
--- percentage w/ different start/end

WITH 
taxi_trips AS (
    SELECT
        weekday(pickup_datetime) as weekday,
        pulocationid, 
        dolocationid
    FROM {{ ref('mart__fact_all_taxi_trips') }}
),
trip_locations AS (
    SELECT
        t.weekday,
        s.borough AS start_borough,
        e.borough AS end_borough
    FROM taxi_trips t
    JOIN {{ ref('mart__dim_locations') }} s ON t.pulocationid = s.locationid 
    JOIN {{ ref('mart__dim_locations') }} e ON t.dolocationid = e.locationid 
),

aggregated_data AS (
    SELECT
        weekday,
        COUNT(*) AS taxi_trips,
        COUNT(*) FILTER (WHERE start_borough <> end_borough) AS diff_borough_trips
    FROM trip_locations
    GROUP BY weekday
)

SELECT
    weekday,
    taxi_trips,
    diff_borough_trips,
    ROUND((diff_borough_trips::FLOAT / taxi_trips) * 100, 2) AS percent_diff_start_end
FROM aggregated_data
ORDER BY weekday

--- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/inter_borough_taxi_trips_by_weekday.sql" > answers/inter_borough_trips.txt