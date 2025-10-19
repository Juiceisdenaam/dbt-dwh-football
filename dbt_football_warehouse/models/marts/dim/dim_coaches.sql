select
    coach_id,
    name,
    gender,
    height,
    weight,
    city_id,
    lastname,
    firstname,
    player_id,
    country_id,
    common_name,
    display_name,
    date_of_birth,
    nationality_id
from {{ ref('stg_coaches') }}