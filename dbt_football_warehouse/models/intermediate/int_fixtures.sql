-- models/intermediate/int_fixtures.sql
-- Grain: één record per fixture_id (wedstrijd)
-- Combineert stg_fixtures + teams + refs + scores + coaches + formations

with fixtures as (
    select * from {{ ref('stg_fixtures') }}
),

teams as (
    select
        fixture_id,
        team_id,
        location,
        case when location = 'home' then team_id end as home_team_id,
        case when location = 'away' then team_id end as away_team_id
    from {{ ref('stg_fixtures_teams') }}
),

teams_pivot as (
    select
        fixture_id,
        max(home_team_id) as home_team_id,
        max(away_team_id) as away_team_id
    from teams
    group by fixture_id
),

-- Referees: 1 row per fixture + type_id (6=ref,7=1st assistant,8=2nd assistant,9=4th official)
referees as (
    select
        fixture_id,
        case when type_id = 6 then referee_id end as referee_id,
        case when type_id = 7 then referee_id end as assistant_referee_1_id,
        case when type_id = 8 then referee_id end as assistant_referee_2_id,
        case when type_id = 9 then referee_id end as fourth_official_id
    from {{ ref('stg_fixtures_referees') }}
),
referees_pivot as (
    select
        fixture_id,
        max(referee_id) as referee_id,
        max(assistant_referee_1_id) as assistant_referee_1_id,
        max(assistant_referee_2_id) as assistant_referee_2_id,
        max(fourth_official_id) as fourth_official_id
    from referees
    group by fixture_id
),

-- Coaches
coaches as (
    select
        c.fixture_id,
        c.team_id,
        c.coach_id,
        t.location  -- 'home' of 'away' komt van stg_fixtures_teams
    from {{ ref('stg_fixtures_coaches') }} c
    left join {{ ref('stg_fixtures_teams') }} t
        on c.fixture_id = t.fixture_id
        and c.team_id = t.team_id
),

coaches_pivot as (
    select
        fixture_id,
        max(case when location = 'home' then coach_id end) as home_coach_id,
        max(case when location = 'away' then coach_id end) as away_coach_id
    from coaches
    group by fixture_id
),

-- Formations
formations as (
    select
        fixture_id,
        case when location = 'home' then formation end as home_formation,
        case when location = 'away' then formation end as away_formation
    from {{ ref('stg_fixtures_formations') }}
),
formations_pivot as (
    select
        fixture_id,
        max(home_formation) as home_formation,
        max(away_formation) as away_formation
    from formations
    group by fixture_id
),

-- Scores
scores as (
    select
        fixture_id,
        case when participant_type = 'home' and type_id = 1525 then goals end as home_goals,
        case when participant_type = 'away' and type_id = 1525 then goals end as away_goals
    from {{ ref('stg_fixtures_scores') }}
),
scores_pivot as (
    select
        fixture_id,
        max(home_goals) as home_goals,
        max(away_goals) as away_goals
    from scores
    group by fixture_id
),

-- TV stations (meerdere per fixture → long table join later)
tvstations as (
    select fixture_id, tvstation_id
    from {{ ref('stg_fixtures_tvstations') }}
),

-- Combine everything
combined as (
    select
        f.fixture_id,
        f.season_id,
        f.league_id,
        f.round_id,
        f.stage_id,
        f.venue_id,

        t.home_team_id,
        t.away_team_id,

        sc.home_goals,
        sc.away_goals,

        c.home_coach_id,
        c.away_coach_id,

        fm.home_formation,
        fm.away_formation,

        r.referee_id,
        r.assistant_referee_1_id,
        r.assistant_referee_2_id,
        r.fourth_official_id

    from fixtures f
    left join teams_pivot t on f.fixture_id = t.fixture_id
    left join scores_pivot sc on f.fixture_id = sc.fixture_id
    left join coaches_pivot c on f.fixture_id = c.fixture_id
    left join formations_pivot fm on f.fixture_id = fm.fixture_id
    left join referees_pivot r on f.fixture_id = r.fixture_id
)

select * from combined