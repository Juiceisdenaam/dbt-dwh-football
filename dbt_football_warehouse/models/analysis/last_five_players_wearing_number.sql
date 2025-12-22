WITH filtered_squad AS (
    SELECT
        s.player_id,
        s.jersey_number,
        l.league_name,
        se.season_name,
        se.season_start_date
    FROM {{ ref('dim_squads') }} AS s
    JOIN {{ ref('dim_seasons') }} AS se 
        ON s.season_id = se.season_id
    JOIN {{ ref('dim_leagues') }} AS l 
        ON se.league_id = l.league_id
    WHERE s.has_values = TRUE
      AND s.jersey_number = '{{ var("jersey_number", 10) }}'
      AND s.team_id = (
          SELECT team_id 
          FROM {{ ref('dim_teams') }}
          WHERE team_name = '{{ var("team_name", "Ajax") }}'
          LIMIT 1
      )
),
ranked AS (
    SELECT
        f.*,
        ROW_NUMBER() OVER (
            PARTITION BY player_id 
            ORDER BY season_start_date DESC
        ) AS rn
    FROM filtered_squad AS f
)
SELECT
    p.player_display_name AS display_name,
    r.jersey_number,
    r.league_name,
    r.season_name
FROM ranked AS r
JOIN {{ ref('dim_players') }} AS p 
    ON r.player_id = p.player_id
WHERE r.rn = 1
ORDER BY r.season_start_date DESC
LIMIT 6
