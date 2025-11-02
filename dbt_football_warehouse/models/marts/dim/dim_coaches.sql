select
    coach_id,
    name as coach_name,
    gender,
    height as coach_height,
    weight as coach_weight,
    city_id,
    lastname as last_name,
    firstname as first_name,
    player_id,
    country_id,
    common_name,
    display_name,
    date_of_birth,
    nationality_id
from {{ ref('stg_coaches') }}