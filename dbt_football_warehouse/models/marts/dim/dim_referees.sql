
SELECT
    referee_id,
    name as referee_name,
    gender,
    height as referee_height,
    weight as referee_weight,
    city_id,
    lastname as last_name,
    firstname as first_name,
    country_id,
    common_name,
    display_name,
    date_of_birth
FROM
    {{ ref('stg_referees') }}