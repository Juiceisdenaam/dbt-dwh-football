select
  (data ->> 'id')::int as team_id,
  data ->> 'name' as name,
  data ->> 'type' as type,
  data ->> 'founded' as founded,
  (data ->> 'venue_id')::int as venue_id,
  (data ->> 'country_id')::int as country_id,
  data ->> 'short_code' as short_code
from {{ source('ingestion', 'sportmonks_src_teams') }}