select
  venue_id,
  name,
  address,
  city_id,
  surface,
  zipcode,
  capacity,
  latitude,
  longitude,
  city_name,
  country_id,
  national_team
from {{ ref('stg_venues') }}