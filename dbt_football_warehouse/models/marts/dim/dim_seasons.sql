select
    season_id,
    name,
    pending,
    finished,
    ending_at,
    league_id,
    is_current,
    starting_at
from {{ ref('stg_seasons') }}