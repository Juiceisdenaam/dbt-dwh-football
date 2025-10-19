with base as (

    select
        data as type
    from {{ source('ingestion', 'sportmonks_src_types') }}

),

flattened as (

    select
        -- Surrogaat sleutel
        md5((type ->> 'id')) as type_sk,

        -- Natuurlijke sleutel
        (type ->> 'id')::int as type_id,

        -- Attributen
        (type ->> 'code') as code,
        (type ->> 'name') as name,
        (type ->> 'model_type') as model_type,
        (type ->> 'stat_group') as stat_group,
        (type ->> 'developer_name') as developer_name

    from base
)

select *
from flattened
