with src AS (
    select 
        id::int as fixture_id,
        jsonb_array_elements(data -> 'coaches') as coach
    from {{ source('ingestion', 'sportmonks_src_fixtures') }}
),

flattened AS (
    SELECT
        md5(concat_ws('-', fixture_id::text, coach ->> 'id')) as fixture_coach_sk,
        fixture_id,
        (coach ->> 'id')::int AS coach_id,
        ((coach -> 'meta') ->> 'participant_id')::int AS team_id
    FROM src
)
SELECT * from flattened