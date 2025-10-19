select
  (data ->> 'id')::int as venue_id,
  data ->> 'name' as name,
  data ->> 'address' as address,
  (data ->> 'city_id')::int as city_id,
  data ->> 'surface' as surface,
  data ->> 'zipcode' as zipcode,
  data ->> 'capacity' as capacity,
  data ->> 'latitude' as latitude,
  data ->> 'longitude' as longitude,
  data ->> 'city_name' as city_name,
  (data ->> 'country_id')::int as country_id,
  data ->> 'national_team' as national_team
from {{ source('ingestion', 'sportmonks_src_venues') }}