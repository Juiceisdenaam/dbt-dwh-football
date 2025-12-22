SELECT
    squad_sk,
    squad_id,
    team_id,
    player_id,
    season_id,
    has_values,
    position_id,
    jersey_number
FROM {{ ref('stg_squads') }}