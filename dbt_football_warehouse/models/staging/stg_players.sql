SELECT
    (data ->> 'id')::int                AS player_id,
    (data ->> 'firstname')              AS player_first_name,
    (data ->> 'lastname')               AS player_last_name,
    (data ->> 'common_name')            AS player_common_name,
    (data ->> 'display_name')           AS player_display_name,
    (data ->> 'name')                   AS player_full_name,
    (data ->> 'gender')                 AS player_gender,
    (data ->> 'height')::int            AS player_height_cm,
    (data ->> 'weight')::int            AS player_weight_kg,
    (data ->> 'date_of_birth')::date    AS player_date_of_birth,
    (data ->> 'city_id')::int           AS player_city_id,
    (data ->> 'country_id')::int        AS player_country_id,
    (data ->> 'nationality_id')::int    AS player_nationality_id,
    (data ->> 'type_id')::int           AS player_type_id,
    (data ->> 'position_id')::int       AS player_position_id
FROM {{ source('ingestion', 'sportmonks_src_players') }}
