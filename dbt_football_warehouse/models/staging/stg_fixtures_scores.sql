WITH src AS (
    SELECT
        id::int AS fixture_id,
        jsonb_array_elements(data -> 'scores') AS score
    FROM {{ source('ingestion', 'sportmonks_src_fixtures') }}
),
flattened AS (
    SELECT
        md5(concat_ws('-', fixture_id::text, score ->> 'id')) AS fixture_score_sk,
        fixture_id,
        (score ->> 'id')::int                AS score_id,
        (score ->> 'type_id')::int           AS type_id,
        (score ->> 'participant_id')::int    AS team_id,
        (score -> 'score' ->> 'goals')::int  AS goals,
        (score -> 'score' ->> 'participant') AS participant_type, -- 'home' / 'away'
        (score ->> 'description')            AS description,
        score                                AS raw_score
    FROM src
)
SELECT * FROM flattened
