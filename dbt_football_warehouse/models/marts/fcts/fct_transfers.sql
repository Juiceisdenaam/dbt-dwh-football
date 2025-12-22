SELECT
    transfer_id,
    transfer_date,
    transfer_amount,
    transfer_type_id,
    sport_id,
    is_completed,
    career_ended,
    player_id,
    from_team_id,
    to_team_id,
    position_id,
    detailed_position_id
FROM {{ ref('stg_transfers') }}