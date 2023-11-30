--- Calculate the number of trips and average duration by borough and zone

with trips as (
    select 
        PUlocationID as id,
        sum(duration_sec) as total_duration,
        count(*) as trip_count
    from {{ref('mart__fact_all_taxi_trips')}}
    group by 1
),

location as (
    select 
        LocationID::varchar as LocationID,
        Borough,
        Zone  
    from {{ref('mart__dim_locations')}}
),

-- CTE for calculating total trips and total duration by Borough and Zone
total_trips as (
    select 
        l.Borough, -- Borough name
        l.zone, -- Zone name
        sum(t.trip_count) as total_trips, -- Sum of trips by Borough and Zone
        sum(t.total_duration) as total_duration -- Sum of duration by Borough and Zone
    from 
        location as l
    right join 
        trips as t on l.LocationID = t.id -- Joining location with trips
    where 
        l.Borough is not null and l.zone is not null -- Filtering non-null Borough and Zone
    group by l.Borough, l.zone
)

select 
    Borough, 
    zone, 
    total_trips, 
    total_duration / total_trips as average_duration
from 
    total_trips



-- first use dbt compile and sql will be located in target
-- then run 
-- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/trips_duration_grouped_by_borough_zone.sql" > answers/trips_duration_grouped_by_borough_zone.txt