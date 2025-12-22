SELECT
        season_id,
        season_name,
        is_pending,
        is_finished,
        season_start_date,
        season_end_date,
        league_id,
        is_current
FROM {{ ref('stg_seasons') }}