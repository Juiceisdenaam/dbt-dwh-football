SELECT
  country_id,
  country_name,
  iso2,
  iso3,
  continent_id,
  official_name,
  fifa_name,
  latitude as country_latitude,
  longitude as country_longitude
FROM
    {{ ref('stg_countries') }}