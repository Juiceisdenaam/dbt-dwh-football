select
  venue_id,
  name as venue_name,
  address as venue_address,
  city_id,
  surface,
  zipcode,
  capacity as venue_capacity,
  latitude as venue_latitude,
  longitude as venue_longitude,
  city_name,
  country_id,
  national_team
from {{ ref('stg_venues') }}