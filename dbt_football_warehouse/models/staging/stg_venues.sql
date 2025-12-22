SELECT
    (data ->> 'id')::int                AS venue_id,
    (data ->> 'name')                   AS venue_name,
    (data ->> 'address')                AS venue_address,
    (data ->> 'zipcode')                AS venue_zipcode,
    (data ->> 'surface')                AS venue_surface,
    (data ->> 'capacity')::int          AS venue_capacity,
    (data ->> 'latitude')               AS venue_latitude,
    (data ->> 'longitude')              AS venue_longitude,
    (data ->> 'city_name')              AS venue_city_name,
    (data ->> 'national_team')::boolean AS is_national_team,
    (data ->> 'city_id')::int           AS venue_city_id,
    (data ->> 'country_id')::int        AS venue_country_id
FROM {{ source('ingestion', 'sportmonks_src_venues') }}
