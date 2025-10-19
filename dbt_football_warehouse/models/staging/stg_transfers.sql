select
  (data ->> 'id')::int                       as transfer_id,
  (data ->> 'date')::date                    as transfer_date,
  (data ->> 'amount')::numeric               as transfer_amount,
  (data ->> 'type_id')::int                  as type_id,
  (data ->> 'sport_id')::int                 as sport_id,
  (data ->> 'completed')::boolean            as is_completed,
  (data ->> 'player_id')::int                as player_id,
  (data ->> 'to_team_id')::int               as to_team_id,
  (data ->> 'from_team_id')::int             as from_team_id,
  (data ->> 'position_id')::int              as position_id,
  (data ->> 'detailed_position_id')::int     as detailed_position_id,
  (data ->> 'career_ended')::boolean         as career_ended
from {{ source('ingestion', 'sportmonks_src_transfers') }}