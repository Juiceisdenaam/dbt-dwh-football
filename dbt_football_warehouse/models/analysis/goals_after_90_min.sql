{{ config(
    materialized='table'
) }}

select
    fixture_id
from {{ ref('fct_events') }} e
-- join {{ ref('fct_fixtures') }} f on e.fixture_id = f.fixture_id
-- join {{ ref('dim_teams') }} t on e.team_id = t.team_id
-- join {{ ref('dim_seasons') }} se on f.season_id = se.season_id
-- join {{ ref('dim_leagues') }} lg on f.league_id = lg.league_id
-- where LOWER(e.type_name) = 'goal'
-- --  and e.minute > 90
--   and lg.league_name = '{{ var("league_name", "Eredivisie") }}'
--   and se.season_name = '{{ var("season_year", "2023/2024") }}'
-- --group by t.team_name, lg.league_name, se.season_year
-- --order by goals_after_90 desc