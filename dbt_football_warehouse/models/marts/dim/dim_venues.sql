SELECT
    venue_id,
    venue_name,
    venue_address,
    venue_zipcode,
    venue_surface,
    venue_capacity,
    venue_latitude,
    venue_longitude,
    venue_city_name,
    is_national_team,
    venue_city_id,
    venue_country_id
FROM {{ ref('stg_venues') }}