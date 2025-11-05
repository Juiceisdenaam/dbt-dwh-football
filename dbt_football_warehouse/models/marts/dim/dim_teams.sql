SELECT
      team_id,
      team_name,
      team_type,
      team_founded_year,
      team_short_code,
      venue_id,
      team_country_id
FROM {{ ref('stg_teams') }}