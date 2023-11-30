--- average_time_between_pickups.sql
--- Find the average time between taxi pick ups per zone
--- Use lead or lag to find the next trip per zone for each record
--- then find the time difference between the pick up time for the current record and the next
--- then use this result to calculate the average time between pick ups per zone

with location as (
select 
    LocationID::varchar as LocationID,
    Zone  
from {{ref('mart__dim_locations')}} ),

combined as (
    select *
    from {{ ref('mart__fact_all_taxi_trips')}} as trip
    inner join location as l
    on trip.pulocationid = l.LocationID
),

nexttrip AS (
    SELECT
        zone,
        pickup_datetime,
        LEAD(pickup_datetime) OVER (PARTITION BY zone ORDER BY pickup_datetime) AS next_pickup_datetime
    FROM combined
),
timediff AS (
    SELECT
        zone,
        pickup_datetime,
        next_pickup_datetime,
        datediff('second', pickup_datetime, next_pickup_datetime) AS time_diff_seconds
    FROM NextTrip
)
SELECT
    zone,
    AVG(time_diff_seconds) AS avg_time_diff_seconds
FROM timediff
GROUP BY zone

--- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/average_time_between_pickups.sql" > answers/average_time_between_pickups.txt