SELECT
      country_id,
      country_name,
      iso2,
      iso3,
      continent_id,
      country_official_name,
      country_fifa_name,
      country_latitude,
      country_longitude
FROM
    {{ ref('stg_countries') }}