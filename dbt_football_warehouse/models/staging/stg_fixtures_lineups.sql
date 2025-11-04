WITH src AS (
    SELECT
        id::int AS fixture_id,
        jsonb_array_elements(data -> 'lineups') AS lineup
    FROM {{ source('ingestion', 'sportmonks_src_fixtures') }}
),
flattened AS (
    SELECT
        md5(concat_ws('-', fixture_id::text, lineup ->> 'player_id')) AS fixture_lineup_sk,
        fixture_id,
        (lineup ->> 'player_id')::int       AS player_id,
        (lineup ->> 'team_id')::int         AS team_id,
        (lineup ->> 'type_id')::int         AS type_id,
        (lineup ->> 'position_id')::int     AS position_id,
        (lineup ->> 'jersey_number')::int   AS jersey_number,
        (lineup ->> 'formation_field')      AS formation_field,
        (lineup ->> 'formation_position')::int AS formation_position
    FROM src
)
SELECT * FROM flattened
