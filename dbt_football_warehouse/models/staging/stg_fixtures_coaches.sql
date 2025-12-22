WITH src AS (
    SELECT
        id::int AS fixture_id,
        jsonb_array_elements(data -> 'coaches') AS coach
    FROM {{ source('ingestion', 'sportmonks_src_fixtures') }}
),

flattened AS (
    SELECT
        -- Surrogate key for uniqueness (fixture + coach)
        md5(concat_ws(
            '-', 
            fixture_id::text, 
            (coach ->> 'id')
        )) AS fixture_coach_sk,

        fixture_id,
        (coach ->> 'id')::int AS coach_id,
        (coach -> 'meta' ->> 'participant_id')::int AS team_id
    FROM src
)

SELECT *
FROM flattened