WITH events AS (
    SELECT
        e.fixture_id,
        e.event_id,
        e.minute,
        e.result,
        p.player_display_name,
        t.team_name,
        se.season_name,
        lg.league_name,
        CAST(split_part(result, '-', 1) AS INT) as home_score_after,
        CAST(split_part(result, '-', 2) AS INT) as away_score_after
    FROM {{ ref('fct_events') }} e
    JOIN {{ ref('fct_fixtures') }} f on e.fixture_id = f.fixture_id
    JOIN {{ ref('dim_teams') }} t on e.team_id = t.team_id
    JOIN {{ ref('dim_players') }} p on e.player_id = p.player_id
    JOIN {{ ref('dim_seasons') }} se on f.season_id = se.season_id
    JOIN {{ ref('dim_leagues') }} lg on f.league_id = lg.league_id
    WHERE 1 = 1
    AND e.result IS NOT NULL
    AND e.type_name = 'Goal'
), with_previous AS (
    SELECT
        *,
        LAG(home_score_after,1,0) OVER (PARTITION BY fixture_id ORDER BY minute, event_id) AS home_score_before,
        LAG(away_score_after,1,0) OVER (PARTITION BY fixture_id ORDER BY minute, event_id) AS away_score_before
    FROM events
), final AS (
    SELECT
        *,
        CASE
            WHEN ABS(home_score_before - away_score_before) <= 1 AND ABS(home_score_after - away_score_after) <= 1 THEN 1
            ELSE 0
        END AS impactful_goal
    FROM with_previous
)   
SELECT
    player_display_name,
    team_name,
    season_name,
    league_name,
    COUNT(1) AS total_goals,
    SUM(impactful_goal) as impactful_goals,
    SUM(impactful_goal) * 1.0 / COUNT(*) AS pct_impactful
FROM final
WHERE 1 = 1
GROUP BY
    player_display_name,
    team_name,
    season_name,
    league_name
ORDER BY pct_impactful DESC