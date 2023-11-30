--- Write a query to compare an individual fare to the zone, borough and overall
--- average fare using the fare_amount in yellow taxi trip data

with location as (
select 
    LocationID::varchar as LocationID,
    Borough,
    Zone  
from {{ref('mart__dim_locations')}} ),
combined as ( --- join with taxi table and group by borough and zone
    select *
    from {{ ref('stg__yellow_tripdata')}} as trip
    inner join location as l
    on trip.pulocationid = l.LocationID
)

select *,
avg(fare_amount) over(partition by Borough ) as average_fair_by_Borough,
avg(fare_amount) over(partition by zone ) as average_fair_by_zone
from combined

-- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/yellow_taxi_fare_comparison.sql" > answers/yellow_taxi_fare_comparison.txt