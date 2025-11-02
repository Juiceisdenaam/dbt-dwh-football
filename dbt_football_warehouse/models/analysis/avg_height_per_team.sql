/*
Gemiddelde lengte van basisspelers per team,
voor een opgegeven competitie en seizoen.
*/

SELECT
    lg.league_name,
    c.country_name,
    se.season_year,
    st.position,
    t.team_name,
    ROUND(AVG(p.player_height), 1) AS avg_height_cm,
    COUNT(DISTINCT l.player_id) AS unique_players
FROM {{ ref('int_fixtures_lineups') }} l
LEFT JOIN {{ ref('dim_players') }} p 
    ON l.player_id = p.player_id
LEFT JOIN {{ ref('dim_teams') }} t 
    ON l.team_id = t.team_id
LEFT JOIN {{ ref('dim_types') }} ty 
    ON l.type_id = ty.type_id
LEFT JOIN {{ ref('fct_fixtures') }} f 
    ON l.fixture_id = f.fixture_id
LEFT JOIN {{ ref('dim_seasons') }} se
    ON f.season_id = se.season_id
LEFT JOIN {{ ref('dim_leagues') }} lg
    ON f.league_id = lg.league_id
LEFT JOIN {{ ref('dim_countries') }} c
    ON c.country_id = lg.country_id
LEFT JOIN {{ ref('fct_standings') }} st
    ON lg.league_id = st.league_id
    AND t.team_id = st.team_id
    AND se.season_id = st.season_id
WHERE 
    ty.type_id = 11  -- alleen basisspelers
    -- AND lg.league_name = '{{ var("league_name") }}'
    -- AND c.country_name = '{{ var("country_name") }}'
    -- AND se.season_year = '{{ var("season_year") }}'
    AND p.player_height IS NOT NULL
GROUP BY 
    lg.league_name,
    c.country_name,
    se.season_year,
    t.team_name,
    st.position
ORDER BY 
    avg_height_cm DESC