SELECT
    league_id,
    league_name,
    league_type,
    league_sub_type,
    is_active,
    league_category,
    league_short_code,
    has_jerseys,
    league_country_id
FROM
    {{ ref('stg_leagues') }}