
WITH base AS (
    SELECT
        jsonb_array_elements(data) AS squad
    FROM {{ source('ingestion', 'sportmonks_src_squads') }}
),

flattened AS (
    SELECT
        -- Surrogate key
        md5(
            concat_ws(
                '-',
                (squad ->> 'id'),
                (squad ->> 'team_id'),
                (squad ->> 'season_id')
            )
        ) AS squad_sk,

        -- Natural keys
        (squad ->> 'id')::int               AS squad_id,
        (squad ->> 'team_id')::int          AS team_id,
        (squad ->> 'player_id')::int        AS player_id,
        (squad ->> 'season_id')::int        AS season_id,

        -- Attributes
        (squad ->> 'has_values')::boolean   AS has_values,
        (squad ->> 'position_id')::int      AS position_id,
        (squad ->> 'jersey_number')::int    AS jersey_number
    FROM base
)

SELECT *
FROM flattened