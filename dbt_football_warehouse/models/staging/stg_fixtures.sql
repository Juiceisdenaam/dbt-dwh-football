SELECT
    id::int                                AS fixture_id,
    (data ->> 'name')                      AS fixture_name,
    (data ->> 'starting_at')::timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Amsterdam' AS fixture_start_time,
    (data ->> 'starting_at_timestamp')::int AS fixture_start_timestamp,
    (data ->> 'venue_id')::int             AS venue_id,
    (data ->> 'league_id')::int            AS league_id,
    (data ->> 'season_id')::int            AS season_id,
    (data ->> 'stage_id')::int             AS stage_id,
    (data ->> 'state_id')::int             AS state_id,
    (data ->> 'group_id')::int             AS group_id,
    (data ->> 'round_id')::int             AS round_id,
    (data ->> 'placeholder')::boolean      AS is_placeholder,
    (data ->> 'result_info')               AS result_info
FROM {{ source('ingestion', 'sportmonks_src_fixtures') }}
