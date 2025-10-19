select
   transfer_id,
    transfer_date,
    transfer_amount,
    is_completed,
    career_ended,
    player_id,
    from_team_id,
    to_team_id,
    position_id,
    detailed_position_id,
    type_id
from {{ ref('stg_transfers') }}