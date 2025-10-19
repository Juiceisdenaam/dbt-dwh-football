with base as (
    select * from {{ ref('stg_fixtures_events') }}
),

-- voeg alle type-informatie toe (zoals naam, code, description)
types as (
    select 
        type_id,
        name as type_name,
        code as type_code,
        model_type,
        stat_group,
        developer_name
    from {{ ref('stg_types') }}
)
select
    base.fixture_id,
    base.event_id,
    base.sub_type_id,
    base.minute,
    base.extra_minute,
    base.player_id,
    base.related_player_id,
    base.team_id,
    base.info,
    base.result,
    base.addition,
    t.type_id,
    t.type_name,
    t.type_code,
    t.model_type,
    t.stat_group,
    t.developer_name
from base
left join types t on base.type_id = t.type_id