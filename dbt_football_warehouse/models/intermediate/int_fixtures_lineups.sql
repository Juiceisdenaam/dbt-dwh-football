SELECT
        fixture_lineup_sk,
        fixture_id,
        player_id,
        team_id,
        type_id,
        position_id,
        jersey_number,
        formation_field,
        formation_position
FROM {{ ref('stg_fixtures_lineups') }}