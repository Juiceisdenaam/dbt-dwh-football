
-- Extract alle events uit de fixture JSON
with base as (
    select
        id as fixture_id,
        jsonb_array_elements(data -> 'events') as event
    from {{ source('ingestion', 'sportmonks_src_fixtures') }}
),

flattened as (
    select
        fixture_id,
        (event ->> 'id')::int                       as event_id,
        (event ->> 'type_id')::int                  as type_id,
        (event ->> 'sub_type_id')::int              as sub_type_id,
        (event ->> 'minute')::int                   as minute,
        (event ->> 'extra_minute')::int             as extra_minute,
        (event ->> 'player_id')::int                as player_id,
        (event ->> 'related_player_id')::int        as related_player_id,
        (event ->> 'participant_id')::int           as team_id,
        (event ->> 'info')                          as info,
        (event ->> 'result')                        as result,
        (event ->> 'addition')                      as addition
    from base
)
select * from flattened
