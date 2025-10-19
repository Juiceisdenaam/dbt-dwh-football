
SELECT
    referee_id,
    name,
    gender,
    height,
    weight,
    city_id,
    lastname,
    firstname,
    country_id,
    common_name,
    display_name,
    date_of_birth
FROM
    {{ ref('stg_referees') }}