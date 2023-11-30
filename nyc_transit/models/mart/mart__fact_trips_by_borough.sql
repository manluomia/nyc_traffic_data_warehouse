--- This sql aims to calculate the number of trips by each Borough

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
        Borough
    from {{ref('mart__dim_locations')}}
)

-- if not borough was recorded then just put not recorded and sum up count of trips by borough
select ifnull(l.Borough, 'Not recorded') as Borough , sum(trip_count) as number_of_trips
from location as l
right join trips as t
on l.LocationID = t.id 
group by l.Borough
