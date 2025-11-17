
WITH filtered_squad AS (
    SELECT *
    FROM {{ ref('dim_squads') }}
    WHERE jersey_number = '{{ var("jersey_number", 10) }}'
      AND team_id = (SELECT team_id FROM dim_team WHERE team_name = '{{ var("team_name", "Ajax") }}')
)
SELECT p.player_display_name,
       s.jersey_number,
       s.season_name
FROM filtered_squad AS s
JOIN {{ ref('dim_players') }} AS p ON s.player_id = p.player_id
ORDER BY s.season_id DESC
LIMIT 5
