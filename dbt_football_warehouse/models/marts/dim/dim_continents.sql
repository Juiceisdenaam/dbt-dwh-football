SELECT
    continent_id,
    continent_code,
    continent_name
FROM
    {{ ref('stg_continents') }}