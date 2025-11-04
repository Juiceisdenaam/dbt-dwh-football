
WITH base AS (
    SELECT
        data AS tvstation
    FROM {{ source('ingestion', 'sportmonks_src_tv_stations') }}
),

flattened AS (
    SELECT
        -- Natural key
        (tvstation ->> 'id')::int          AS tvstation_id,

        -- Attributes
        (tvstation ->> 'name')             AS tvstation_name,
        (tvstation ->> 'url')              AS tvstation_url,
        (tvstation ->> 'type')             AS tvstation_type,
        (tvstation ->> 'image_path')       AS tvstation_image_path,
        (tvstation ->> 'related_id')::int  AS related_id
    FROM base
)

SELECT *
FROM flattened
