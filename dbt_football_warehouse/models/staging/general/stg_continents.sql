SELECT
      (data ->> 'id')::int  as continent_id,
      (data ->> 'code')     as continent_code,
      (data ->> 'name')     as continent_name
FROM {{ source('ingestion', 'sportmonks_src_continents') }}