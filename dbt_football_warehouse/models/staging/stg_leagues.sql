
SELECT

(data ->> 'id')::int AS league_id,

(data ->> 'name') AS name,

(data ->> 'type') AS type,

(data ->> 'sub_type') AS sub_type,

(data ->> 'active') AS active,

(data ->> 'category') AS category,

(data ->> 'short_code') AS short_code,

(data ->> 'has_jerseys') AS has_jerseys,

(data ->> 'country_id')::int AS country_id

FROM

        {{ source('ingestion', 'sportmonks_src_leagues') }}