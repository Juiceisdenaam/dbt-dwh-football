
with src AS (

select id::int as fixture_id,

	jsonb_array_elements(data -> 'participants') as participant

	from {{ source('ingestion', 'sportmonks_src_fixtures') }}

	),

	flattened AS (

		SELECT

			md5(concat_ws('-', fixture_id::text, participant ->> 'id')) as fixture_team_sk,

			fixture_id,

			(participant ->> 'id')::int AS team_id,

			(participant -> 'meta') ->> 'winner' AS winner,

			(participant -> 'meta') ->> 'location' AS location,

			(participant -> 'meta') ->> 'position' AS position

		FROM src

		)

	SELECT * from flattened