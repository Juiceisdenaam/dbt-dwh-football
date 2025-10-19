
SELECT

(data ->> 'id')::int AS season_id,

(data ->> 'name') AS name,

(data ->> 'pending') AS pending,

(data ->> 'finished') AS finished,

(data ->> 'ending_at') AS ending_at,

(data ->> 'league_id')::int AS league_id,

(data ->> 'is_current') AS is_current,

(data ->> 'starting_at') AS starting_at
FROM

        {{ source('ingestion', 'sportmonks_src_seasons') }}