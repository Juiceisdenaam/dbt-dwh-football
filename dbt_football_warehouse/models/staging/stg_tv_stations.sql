with base as (

    select
        data as tvstation
    from {{ source('ingestion', 'sportmonks_src_tv_stations') }}

),

flattened as (

    select
        -- Surrogaat sleutel
        md5((tvstation ->> 'id')) as tvstation_sk,

        -- Natuurlijke sleutel
        (tvstation ->> 'id')::int as tvstation_id,

        -- Attributen
        (tvstation ->> 'name') as name,
        (tvstation ->> 'url') as url,
        (tvstation ->> 'type') as type,
        (tvstation ->> 'image_path') as image_path,
        (tvstation ->> 'related_id')::int as related_id

    from base
)

select *
from flattened
