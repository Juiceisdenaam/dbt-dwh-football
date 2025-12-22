WITH base AS (
    SELECT
        data AS standing
    FROM {{ source('ingestion', 'sportmonks_src_standings') }}
),

flattened AS (
    SELECT
        -- Surrogate key for uniqueness
        md5(
            concat_ws(
                '-',
                (standing ->> 'id'),
                (standing ->> 'season_id'),
                (standing ->> 'league_id')
            )
        ) AS standing_sk,

        -- Natural keys
        (standing ->> 'id')::int           AS standing_id,
        (standing ->> 'league_id')::int    AS league_id,
        (standing ->> 'season_id')::int    AS season_id,
        (standing ->> 'participant_id')::int AS team_id,

        -- Attributes
        (standing ->> 'points')::int       AS points,
        (standing ->> 'position')::int     AS position,
        (standing ->> 'round_id')::int     AS round_id,
        (standing ->> 'stage_id')::int     AS stage_id,
        (standing ->> 'sport_id')::int     AS sport_id,
        (standing ->> 'standing_rule_id')::int AS standing_rule_id,
        (standing ->> 'result')            AS result,
        (standing ->> 'group_id')::int     AS group_id
    FROM base
)

SELECT *
FROM flattened
