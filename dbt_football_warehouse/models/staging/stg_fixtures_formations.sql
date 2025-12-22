WITH src AS (
    SELECT
        id::int AS fixture_id,
        jsonb_array_elements(data -> 'formations') AS formation
    FROM {{ source('ingestion', 'sportmonks_src_fixtures') }}
),
flattened AS (
    SELECT
        md5(concat_ws('-', fixture_id::text, formation ->> 'id')) AS fixture_formation_sk,
        fixture_id,
        (formation ->> 'id')::int            AS formation_id,
        (formation ->> 'location')           AS formation_location,
        (formation ->> 'participant_id')::int AS team_id,
        (formation ->> 'formation')          AS formation_name
    FROM src
)
SELECT * FROM flattened
