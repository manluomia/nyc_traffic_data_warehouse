WITH start_stations AS (
    SELECT
        start_station_id AS station_id,
        MAX(start_station_name) AS station_name,
        MAX(start_lat) AS station_lat,
        MAX(start_lng) AS station_lng
    FROM {{ ref('stg__bike_data') }}
    GROUP BY start_station_id
), end_stations AS (
    SELECT
        end_station_id AS station_id,
        MAX(end_station_name) AS station_name,
        MAX(end_lat) AS station_lat,
        MAX(end_lng) AS station_lng
    FROM {{ ref('stg__bike_data') }}
    GROUP BY end_station_id
), combined_stations AS (
    SELECT * FROM start_stations
    UNION
    SELECT * FROM end_stations
)
SELECT
    station_id,
    MAX(station_name) AS station_name,
    MAX(station_lat) AS station_lat,
    MAX(station_lng) AS station_lng
FROM combined_stations
GROUP BY station_id
