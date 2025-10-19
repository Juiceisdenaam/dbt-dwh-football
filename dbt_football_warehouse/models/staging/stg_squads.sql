with base as (

    -- Haal elk squad record uit de JSON-array
    select
        jsonb_array_elements(data) as squad
    from {{ source('ingestion', 'sportmonks_src_squads') }}

),

flattened as (

    select
        -- Surrogaat sleutel
        md5(
            concat_ws(
                '-',
                (squad ->> 'id'),
                (squad ->> 'team_id'),
                (squad ->> 'season_id')
            )
        ) as squad_sk,

        -- Natuurlijke sleutels
        (squad ->> 'id')::int as squad_id,
        (squad ->> 'team_id')::int as team_id,
        (squad ->> 'player_id')::int as player_id,
        (squad ->> 'season_id')::int as season_id,

        -- Attributen
        (squad ->> 'has_values')::boolean as has_values,
        (squad ->> 'position_id')::int as position_id,
        (squad ->> 'jersey_number')::int as jersey_number

    from base
)

select *
from flattened