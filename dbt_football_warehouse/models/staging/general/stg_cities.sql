select
  (data ->> 'id')::int as city_id,
  data ->> 'name' as city_name,
  data ->> 'country_id' as country_id,
  data ->> 'region_id' as region_id,
  data ->> 'latitude' as latitude,
  data ->> 'longitude' as longitude
from {{ source('ingestion', 'sportmonks_src_cities') }}