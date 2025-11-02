with src AS (

    select 
        id::int as fixture_id,
        jsonb_array_elements(data -> 'scores') as score
    from {{ source('ingestion', 'sportmonks_src_fixtures') }}

),

flattened AS (

    select
        md5(concat_ws('-', fixture_id::text, score ->> 'id')) as fixture_score_sk,
        fixture_id,
        (score ->> 'id')::int as score_id,
        (score ->> 'type_id')::int as type_id,
        (score ->> 'participant_id')::int as team_id,
        (score -> 'score' ->> 'goals')::int as goals,
        score -> 'score' ->> 'participant' as participant_type, -- 'home' / 'away'
        score ->> 'description' as description,
        score as raw_score
    from src

)

select * from flattened
