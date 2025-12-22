SELECT
    (data ->> 'id')::int               AS referee_id,
    (data ->> 'firstname')             AS referee_first_name,
    (data ->> 'lastname')              AS referee_last_name,
    (data ->> 'common_name')           AS referee_common_name,
    (data ->> 'display_name')          AS referee_display_name,
    (data ->> 'name')                  AS referee_full_name,
    (data ->> 'gender')                AS referee_gender,
    (data ->> 'date_of_birth')::date   AS referee_date_of_birth,
    (data ->> 'height')::int           AS referee_height_cm,
    (data ->> 'weight')::int           AS referee_weight_kg,
    (data ->> 'city_id')::int          AS referee_city_id,
    (data ->> 'country_id')::int       AS referee_country_id
FROM {{ source('ingestion', 'sportmonks_src_referees') }}
