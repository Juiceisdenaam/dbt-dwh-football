select
    season_id,
    name as season_year,
    pending,
    finished,
    ending_at,
    league_id,
    is_current,
    starting_at
from {{ ref('stg_seasons') }}