with source as (

    select * from {{ source('main', 'fhv_tripdata') }}

),

renamed as (

    select
        trim(upper(dispatching_base_num)) as  dispatching_base_num, --some ids are lowercase
        pickup_datetime,
        dropoff_datetime,
        pulocationid::int as pulocationid ,
        dolocationid::int as dolocationid,
        --sr_flag, always null so chuck it
        trim(upper(affiliated_base_number)) as affiliated_base_number,
        filename

    from source

)

select * from renamed