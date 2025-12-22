SELECT
      (data ->> 'id')::int          as region_id,
      (data ->> 'name')             as region_name,
      (data ->> 'country_id')::int  as country_id
FROM {{ source('ingestion', 'sportmonks_src_regions') }}