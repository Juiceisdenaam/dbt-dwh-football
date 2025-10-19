select
  team_id,
  name,
  type,
  founded,
  venue_id,
  country_id,
  short_code
from {{ ref('stg_teams') }}