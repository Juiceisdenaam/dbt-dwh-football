SELECT
        referee_id,
        referee_first_name,
        referee_last_name,
        referee_common_name,
        referee_display_name,
        referee_full_name,
        referee_gender,
        referee_date_of_birth,
        referee_height_cm,
        referee_weight_kg,
        referee_city_id,
        referee_country_id
FROM
    {{ ref('stg_referees') }}