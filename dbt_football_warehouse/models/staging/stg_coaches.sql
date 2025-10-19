SELECT

(data ->> 'id')::int AS coach_id,

(data ->> 'name') AS name,

(data ->> 'gender') AS gender,

(data ->> 'height') AS height,

(data ->> 'weight') AS weight,

(data ->> 'city_id')::int AS city_id,

(data ->> 'lastname') AS lastname,

(data ->> 'firstname') AS firstname,

(data ->> 'player_id')::int AS player_id,

(data ->> 'country_id')::int AS country_id,

(data ->> 'common_name') AS common_name,

(data ->> 'display_name') AS display_name,

(data ->> 'date_of_birth') AS date_of_birth,

(data ->> 'nationality_id')::int AS nationality_id

FROM

        {{ source('ingestion', 'sportmonks_src_coaches') }}