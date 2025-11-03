SELECT
        (data ->> 'id')::int AS coach_id,
        (data ->> 'name') AS coach_name,
        (data ->> 'gender') AS gender,
        (data ->> 'height') AS height_cm,
        (data ->> 'weight') AS weight_kg,
        (data ->> 'city_id')::int AS city_id,
        (data ->> 'lastname') AS coach_last_name,
        (data ->> 'firstname') AS coach_first_name,
        (data ->> 'player_id')::int AS player_id,
        (data ->> 'country_id')::int AS country_id,
        (data ->> 'common_name') AS coach_common_name,
        (data ->> 'display_name') AS coach_display_name,
        (data ->> 'date_of_birth') AS coach_date_of_birth,
        (data ->> 'nationality_id')::int AS nationality_id
FROM
        {{ source('ingestion', 'sportmonks_src_coaches') }}