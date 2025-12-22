SELECT
      (data ->> 'id')::int          as city_id,
      (data ->> 'name')             as city_name,
      (data ->> 'country_id')::int  as country_id,
      (data ->> 'region_id')::int   as region_id,
      (data ->> 'latitude')         as city_latitude,
      (data ->> 'longitude')        as city_longitude
FROM {{ source('ingestion', 'sportmonks_src_cities') }}