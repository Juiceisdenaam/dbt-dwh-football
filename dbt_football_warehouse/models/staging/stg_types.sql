WITH base AS (
    SELECT
        data AS type
    FROM {{ source('ingestion', 'sportmonks_src_types') }}
),

flattened AS (
    SELECT
        -- Natural key
        (type ->> 'id')::int            AS type_id,

        -- Attributes
        (type ->> 'code')               AS type_code,
        (type ->> 'name')               AS type_name,
        (type ->> 'model_type')         AS model_type,
        (type ->> 'stat_group')         AS stat_group,
        (type ->> 'developer_name')     AS developer_name
    FROM base
)

SELECT *
FROM flattened
