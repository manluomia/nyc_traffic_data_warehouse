with trips_renamed as
(
    select 
        'bike' as type,
        started_at_ts,
        ended_at_ts,
        datediff('minute', started_at_ts, ended_at_ts) as duration_min,
        datediff('second', started_at_ts, ended_at_ts) as duration_sec
    from {{ ref('stg__bike_data') }}

    union all

    select 
        'fhv' as type,
        pickup_datetime,
        dropoff_datetime,
        datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
        datediff('second', pickup_datetime, dropoff_datetime) as duration_sec
    from {{ ref('stg__fhv_tripdata') }}

    union all

    select 
        'fhvhv' as type,
        pickup_datetime,
        dropoff_datetime,
        datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
        datediff('second', pickup_datetime, dropoff_datetime) as duration_sec
    from {{ ref('stg__fhvhv_tripdata') }}

    union all

    select 
        'green' as type,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        datediff('minute', lpep_pickup_datetime, lpep_dropoff_datetime) as duration_min,
        datediff('second', lpep_pickup_datetime, lpep_dropoff_datetime) as duration_sec
    from {{ ref('stg__green_tripdata')}}

    union all

    select 
        'yellow' as type,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as duration_min,
        datediff('second', tpep_pickup_datetime, tpep_dropoff_datetime) as duration_sec
    from {{ ref('stg__yellow_tripdata')}}
)

select *
from trips_renamed