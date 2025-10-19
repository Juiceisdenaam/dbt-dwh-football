SELECT
  city_id,
  city_name,
  country_id,
  region_id,
  latitude,
  longitude
FROM
    {{ ref('stg_cities') }}