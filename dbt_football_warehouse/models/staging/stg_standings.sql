with base as (

    select
        data as standing
    from {{ source('ingestion', 'sportmonks_src_standings') }}

),

flattened as (

    select
        -- Surrogaat sleutel
        md5(
            concat_ws(
                '-',
                (standing ->> 'id'),
                (standing ->> 'season_id'),
                (standing ->> 'league_id')
            )
        ) as standing_sk,

        -- Natuurlijke sleutels
        (standing ->> 'id')::int as standing_id,
        (standing ->> 'league_id')::int as league_id,
        (standing ->> 'season_id')::int as season_id,
        (standing ->> 'participant_id')::int as team_id,

        -- Attributen
        (standing ->> 'points')::int as points,
        (standing ->> 'position')::int as position,
        (standing ->> 'round_id')::int as round_id,
        (standing ->> 'stage_id')::int as stage_id,
        (standing ->> 'sport_id')::int as sport_id,
        (standing ->> 'standing_rule_id')::int as standing_rule_id,
        (standing ->> 'result') as result,
        (standing ->> 'group_id')::int as group_id

    from base
)

select *
from flattened