{{ config(
    materialized='table'
) }}

with filtered_fixtures as (
    select f.fixture_id, f.season_id, f.league_id
    from {{ ref('fct_fixtures') }} f
    join {{ ref('dim_seasons') }} se on f.season_id = se.season_id
    join {{ ref('dim_leagues') }} lg on f.league_id = lg.league_id
    where lg.league_name = '{{ var("league_name", "Eredivisie") }}'
      and se.season_year = '{{ var("season_year", "2023/2024") }}'
),

filtered_lineups as (
    select l.fixture_id, l.team_id, l.player_id
    from {{ ref('int_fixtures_lineups') }} l
    join filtered_fixtures f on l.fixture_id = f.fixture_id
    where l.type_id = 11
),

team_heights as (
    select
        fl.team_id,
        round(avg(p.player_height), 1) as avg_height_cm,
        count(distinct fl.player_id) as unique_players
    from filtered_lineups fl
    join {{ ref('dim_players') }} p on fl.player_id = p.player_id
    where p.player_height is not null
    group by fl.team_id
)

select
    lg.league_name,
    se.season_year,
    st.position,
    t.team_name,
    th.avg_height_cm,
    th.unique_players
from team_heights th
join {{ ref('dim_teams') }} t on th.team_id = t.team_id
left join {{ ref('fct_standings') }} st 
    on st.team_id = t.team_id
   and st.season_id = (select season_id from filtered_fixtures limit 1)
   and st.league_id = (select league_id from filtered_fixtures limit 1)
join {{ ref('dim_leagues') }} lg 
    on lg.league_id = (select league_id from filtered_fixtures limit 1)
join {{ ref('dim_seasons') }} se 
    on se.season_id = (select season_id from filtered_fixtures limit 1)
order by th.avg_height_cm desc
