select
  (data ->> 'id')::int as player_id,
  data ->> 'gender' as gender,
  data ->> 'height' as height,
  data ->> 'weight' as weight,
  (data ->> 'city_id')::int as city_id,
  (data ->> 'type_id')::int as type_id,
  data ->> 'lastname' as last_name,
  data ->> 'firstname' as first_name,
  data ->> 'display_name' as display_name,
  (data ->> 'country_id')::int as country_id,
  (data ->> 'position_id')::int as position_id,
  data ->> 'date_of_birth' as date_of_birth,
  (data ->> 'nationality_id')::int as nationality_id
from {{ source('ingestion', 'sportmonks_src_players') }}