--- Make a query which finds taxi trips which don’t have a pick up location_id in the locations table.
with location as (
    select 
        distinct(LocationID::varchar) as LocationID,
    from {{ref('mart__dim_locations')}}
) 

--- use anti join to filter out PublocationID not in
select 
    *
from {{ref('mart__fact_all_taxi_trips')}} as t
left join location as l
on t.PUlocationID = l.LocationID
where t.PUlocationID is null
-- first use dbt compile and sql will be located in target
-- then run 
-- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/taxi_trips_no_valid_pickup_location_id.sql" > answers/taxi_no_pickup_location.txt