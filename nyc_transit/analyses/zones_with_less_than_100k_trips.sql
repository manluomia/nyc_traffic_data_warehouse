--- Write a query which finds all the Zones where there are less than 100000 trips.

with trips as (
    --- first get sum of count of trips by ID
    select 
        PUlocationID as id,
        count(*) as trip_count
    from {{ref('mart__fact_all_taxi_trips')}}
    group by 1
),

location as (
    select 
        LocationID::varchar as LocationID,
        Zone
    from {{ref('mart__dim_locations')}}
)

select l.Zone as Zone
from location as l
right join trips as t
on l.LocationID = t.id 
where l.Zone is not null
group by l.Zone
having sum(trip_count) <= 100000

-- first use dbt compile and sql will be located in target
-- then run 
-- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/zones_with_less_than_100k_trips.sql" > answers/zones_with_less_than_100k_trips.txt