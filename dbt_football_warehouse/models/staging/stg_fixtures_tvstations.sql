WITH src AS (
    SELECT
        id::int AS fixture_id,
        jsonb_array_elements(data -> 'tvstations') AS tvstation
    FROM {{ source('ingestion', 'sportmonks_src_fixtures') }}
),
flattened AS (
    SELECT
        md5(concat_ws('-', fixture_id::text, tvstation ->> 'tvstation_id')) AS fixture_tvstation_sk,
        fixture_id,
        (tvstation ->> 'country_id')::int    AS country_id,
        (tvstation ->> 'tvstation_id')::int  AS tvstation_id
    FROM src
)
SELECT * FROM flattened
