
SELECT
    (data ->> 'id')::int             AS team_id,
    (data ->> 'name')                AS team_name,
    (data ->> 'type')                AS team_type,
    (data ->> 'founded')::int        AS team_founded_year,
    (data ->> 'short_code')          AS team_short_code,
    (data ->> 'venue_id')::int       AS venue_id,
    (data ->> 'country_id')::int     AS team_country_id
FROM {{ source('ingestion', 'sportmonks_src_teams') }}
