select
  player_id,
  gender,
  height as player_height,
  weight as player_weight,
  city_id,
  type_id,
  last_name,
  first_name,
  display_name,
  country_id,
  position_id,
  date_of_birth,
  nationality_id
from {{ ref('stg_players') }}