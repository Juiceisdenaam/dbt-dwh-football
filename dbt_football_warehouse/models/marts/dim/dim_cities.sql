SELECT
  city_id,
  city_name,
  country_id,
  region_id,
  latitude as city_latitude,
  longitude as city_longitude
FROM
    {{ ref('stg_cities') }}