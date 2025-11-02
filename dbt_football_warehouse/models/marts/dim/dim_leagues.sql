SELECT
    league_id,
    name as league_name,
    type as league_type,
    sub_type as league_sub_type,
    active,
    category as league_category,
    short_code as league_short_code,
    has_jerseys,
    country_id
FROM
    {{ ref('stg_leagues') }}