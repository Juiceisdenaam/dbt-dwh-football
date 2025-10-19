
SELECT

(data ->> 'id')::int AS referee_id,

(data ->> 'name') AS name,

(data ->> 'gender') AS gender,

(data ->> 'height') AS height,

(data ->> 'weight') AS weight,

(data ->> 'city_id')::int AS city_id,

(data ->> 'lastname') AS lastname,

(data ->> 'firstname') AS firstname,

(data ->> 'country_id')::int AS country_id,
(data ->> 'common_name') AS common_name,
(data ->> 'display_name') AS display_name,
(data ->> 'date_of_birth') AS date_of_birth

FROM

        {{ source('ingestion', 'sportmonks_src_referees') }}