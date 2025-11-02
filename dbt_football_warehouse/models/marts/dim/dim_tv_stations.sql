select
    tvstation_id,
    name as tv_station_name,
    url,
    type as tv_station_type,
    related_id
from
    {{ ref('stg_tv_stations') }}