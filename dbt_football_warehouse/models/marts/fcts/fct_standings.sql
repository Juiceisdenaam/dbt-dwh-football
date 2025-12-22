SELECT
    standing_sk,
    standing_id,
    league_id,
    season_id,
    team_id,
    points,
    position,
    round_id,
    stage_id,
    sport_id,
    standing_rule_id,
    result,
    group_id
FROM {{ ref('stg_standings') }}