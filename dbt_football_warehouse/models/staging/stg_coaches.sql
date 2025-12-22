SELECT
        (data ->> 'id')::int                    AS coach_id,
        (data ->> 'firstname')                  AS coach_first_name,
        (data ->> 'lastname')                   AS coach_last_name,
        (data ->> 'common_name')                AS coach_common_name,
        (data ->> 'display_name')               AS coach_display_name,
        (data ->> 'name')                       AS coach_full_name,
        (data ->> 'date_of_birth')::date        AS coach_date_of_birth,
        (data ->> 'gender')                     AS coach_gender,
        (data ->> 'height')::int                AS coach_height_cm,
        (data ->> 'weight')::int                AS coach_weight_kg,
        (data ->> 'city_id')::int               AS coach_city_id,
        (data ->> 'country_id')::int            AS coach_country_id,
        (data ->> 'nationality_id')::int        AS coach_nationality_id,
        (data ->> 'player_id')::int             AS player_id
FROM {{ source('ingestion', 'sportmonks_src_coaches') }}