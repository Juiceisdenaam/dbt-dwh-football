WITH name_counts AS (
    SELECT
        fl.fixture_id,
        p.player_first_name,
        COUNT(*) AS name_count,
        RANK() OVER (PARTITION BY fl.fixture_id ORDER BY COUNT(*) DESC) AS rnk
    FROM {{ ref('int_fixtures_lineups') }} fl
    JOIN {{ ref('dim_players') }} p ON fl.player_id = p.player_id
    WHERE fl.type_id = 11
    GROUP BY fl.fixture_id, p.player_first_name
)
SELECT
    f.fixture_start_time,
    l.league_name,
    s.season_name,
    t_home.team_name AS home_team,
    t_away.team_name AS away_team,
    nc.player_first_name,
    nc.name_count
FROM name_counts nc
JOIN {{ ref('fct_fixtures') }} f
    ON nc.fixture_id = f.fixture_id
JOIN {{ ref('dim_teams') }} t_home
    ON f.home_team_id = t_home.team_id
JOIN {{ ref('dim_teams') }} t_away
    ON f.away_team_id = t_away.team_id
JOIN {{ ref('dim_leagues') }} l
    ON f.league_id = l.league_id
JOIN {{ ref('dim_seasons') }} s
    ON f.season_id = s.season_id
WHERE nc.rnk = 1
ORDER BY nc.name_count DESC