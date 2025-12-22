SELECT
      city_id,
      city_name,
      country_id,
      region_id,
      city_latitude,
      city_longitude
FROM
    {{ ref('stg_cities') }}