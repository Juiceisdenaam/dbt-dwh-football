select
  (data ->> 'id')::int as region_id,
  data ->> 'name' as region_name,
  data ->> 'country_id' as country_id
from {{ source('ingestion', 'sportmonks_src_regions') }}