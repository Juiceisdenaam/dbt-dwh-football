SELECT
    coach_id,
    coach_first_name,
    coach_last_name,
    coach_common_name,
    coach_display_name,
    coach_full_name,
    coach_date_of_birth,
    coach_gender,
    coach_height_cm,
    coach_weight_kg,
    coach_city_id,
    coach_country_id,
    coach_nationality_id
    player_id
FROM {{ ref('stg_coaches') }}