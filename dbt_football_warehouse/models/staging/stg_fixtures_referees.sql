WITH src AS (
    SELECT
        id::int AS fixture_id,
        jsonb_array_elements(data -> 'referees') AS referee
    FROM {{ source('ingestion', 'sportmonks_src_fixtures') }}
),
flattened AS (
    SELECT
        md5(concat_ws('-', fixture_id::text, referee ->> 'referee_id')) AS fixture_referee_sk,
        fixture_id,
        (referee ->> 'referee_id')::int AS referee_id,
        (referee ->> 'type_id')::int    AS type_id
    FROM src
)
SELECT * FROM flattened
