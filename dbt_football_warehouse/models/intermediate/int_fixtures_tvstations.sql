SELECT
fixture_id,
country_id,
tvstation_id
FROM {{ ref('stg_fixtures_tvstations') }}