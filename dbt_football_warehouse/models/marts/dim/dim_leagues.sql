SELECT
    league_id,
    name,
    type,
    sub_type,
    active,
    category,
    short_code,
    has_jerseys,
    country_id
FROM
    {{ ref('stg_leagues') }}