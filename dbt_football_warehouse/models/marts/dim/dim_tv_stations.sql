select
    tvstation_id,
    name,
    url,
    type,
    related_id
from
    {{ ref('stg_tv_stations') }}