WITH base AS (
    SELECT
        id::INT AS fixture_id,
        jsonb_array_elements(data -> 'events') AS event
    FROM {{ source('ingestion', 'sportmonks_src_fixtures') }}
),

flattened AS (
    SELECT
        md5(concat_ws('-', fixture_id::text, event ->> 'id')) AS fixture_event_sk,
        fixture_id,
        (event ->> 'id')::int                       AS event_id,
        (event ->> 'type_id')::int                  AS type_id,
        (event ->> 'sub_type_id')::int              AS sub_type_id,
        (event ->> 'minute')::int                   AS minute,
        (event ->> 'extra_minute')::int             AS extra_minute,
        (event ->> 'player_id')::int                AS player_id,
        (event ->> 'related_player_id')::int        AS related_player_id,
        (event ->> 'participant_id')::int           AS team_id,
        (event ->> 'info')                          AS info,
        (event ->> 'result')                        AS result,
        (event ->> 'addition')                      AS addition
    from base
)
SELECT * FROM flattened