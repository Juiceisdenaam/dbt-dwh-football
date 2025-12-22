SELECT
      region_id,
      region_name,
      country_id
FROM
    {{ ref('stg_regions') }}