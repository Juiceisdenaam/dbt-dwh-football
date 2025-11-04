SELECT
    (data ->> 'id')::int                      AS transfer_id,
    (data ->> 'date')::date                   AS transfer_date,
    (data ->> 'amount')::numeric              AS transfer_amount,
    (data ->> 'type_id')::int                 AS transfer_type_id,
    (data ->> 'sport_id')::int                AS sport_id,
    (data ->> 'completed')::boolean           AS is_completed,
    (data ->> 'career_ended')::boolean        AS career_ended,
    (data ->> 'player_id')::int               AS player_id,
    (data ->> 'from_team_id')::int            AS from_team_id,
    (data ->> 'to_team_id')::int              AS to_team_id,
    (data ->> 'position_id')::int             AS position_id,
    (data ->> 'detailed_position_id')::int    AS detailed_position_id
FROM {{ source('ingestion', 'sportmonks_src_transfers') }}