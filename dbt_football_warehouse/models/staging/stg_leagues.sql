SELECT
    (data ->> 'id')::int                AS league_id,
    (data ->> 'name')                   AS league_name,
    (data ->> 'type')                   AS league_type,
    (data ->> 'sub_type')               AS league_sub_type,
    (data ->> 'active')::boolean        AS is_active,
    (data ->> 'category')               AS league_category,
    (data ->> 'short_code')             AS league_short_code,
    (data ->> 'has_jerseys')::boolean   AS has_jerseys,
    (data ->> 'country_id')::int        AS league_country_id
FROM {{ source('ingestion', 'sportmonks_src_leagues') }}
