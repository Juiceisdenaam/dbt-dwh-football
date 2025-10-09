select
  (data ->> 'id')::int as country_id,
  data ->> 'name' as country_name,
  data ->> 'iso2' as iso2,
  data ->> 'iso3' as iso3,
  (data ->> 'continent_id')::int as continent_id,
  data ->> 'official_name' as official_name,
  data ->> 'fifa_name' as fifa_name,
  data ->> 'latitude' as latitude,
  data ->> 'longitude' as longitude
from {{ source('ingestion', 'sportmonks_src_countries') }}