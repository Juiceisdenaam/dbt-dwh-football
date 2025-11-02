select
  team_id,
  name as team_name,
  type as team_type,
  founded as team_founded_year,
  venue_id,
  country_id,
  short_code as team_short_code
from {{ ref('stg_teams') }}