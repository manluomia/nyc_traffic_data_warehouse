--- taxi_trips_ending_at_airport.sql - total number of trips ending in service_zones 'Airports' or 'EWR'
--- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/taxi_trips_ending_at_airport.sql" > answers/total_taxi_trips_to_airport.txt


WITH trips AS (
    SELECT 
        t.*,
        l.service_zone
    FROM {{ ref('mart__fact_all_taxi_trips') }} t
    JOIN {{ ref('mart__dim_locations') }} l ON t.dolocationid = l.locationid
)

SELECT 
    COUNT(*) AS total_trips
FROM trips
WHERE 
    service_zone IN ('Airports', 'EWR')