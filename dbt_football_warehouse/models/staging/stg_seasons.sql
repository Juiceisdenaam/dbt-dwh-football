SELECT
    (data ->> 'id')::int             AS season_id,
    (data ->> 'name')                AS season_name,
    (data ->> 'pending')::boolean    AS is_pending,
    (data ->> 'finished')::boolean   AS is_finished,
    (data ->> 'starting_at')::date   AS season_start_date,
    (data ->> 'ending_at')::date     AS season_end_date,
    (data ->> 'league_id')::int      AS league_id,
    (data ->> 'is_current')::boolean AS is_current
FROM {{ source('ingestion', 'sportmonks_src_seasons') }}
