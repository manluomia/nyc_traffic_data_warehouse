SELECT 
    *
FROM {{ ref('events')}}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id, user_id 
    ORDER BY event_timestamp DESC
) = 1;

-- duckdb main.db -s ".read nyc_transit/target/compiled/nyc_transit/analyses/dedupe.sql" > answers/dedupe.txt