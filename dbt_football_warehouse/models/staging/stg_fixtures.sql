select

	id as fixture_id,
	data ->> 'name' AS fixture_name,
	(data ->> 'starting_at')::timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Amsterdam' AS starting_at,
	(data ->> 'venue_id')::int AS venue_id,
	(data ->> 'league_id')::int AS league_id,
	(data ->> 'season_id')::int AS season_id,
	(data ->> 'stage_id')::int AS stage_id,
	(data ->> 'state_id')::int AS state_id,
	(data ->> 'group_id')::int AS group_id,
	(data ->> 'round_id')::int AS round_id,
	(data ->> 'placeholder')::boolean AS placeholder,
	data ->> 'result_info' AS result_info,
	(data ->> 'starting_at_timestamp')::int AS starting_at_timestamp

from {{ source('ingestion', 'sportmonks_src_fixtures') }}