WITH src AS (
    SELECT
        id::int AS fixture_id,
        jsonb_array_elements(data -> 'participants') AS participant
    FROM {{ source('ingestion', 'sportmonks_src_fixtures') }}
),
flattened AS (
    SELECT
        md5(concat_ws('-', fixture_id::text, participant ->> 'id')) AS fixture_team_sk,
        fixture_id,
        (participant ->> 'id')::int                AS team_id,
        (participant -> 'meta' ->> 'winner')       AS winner,
        (participant -> 'meta' ->> 'location')     AS location,
        (participant -> 'meta' ->> 'position')     AS position
    FROM src
)
SELECT * FROM flattened
