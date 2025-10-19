with src AS (

select id::int as fixture_id,

	jsonb_array_elements(data -> 'formations') as formation

	from {{ source('ingestion', 'sportmonks_src_fixtures') }}

	),

	flattened AS (

		SELECT

			md5(concat_ws('-', fixture_id::text, formation ->> 'id')) as fixture_formation_sk,

			fixture_id,

			(formation ->> 'location') AS location,

			(formation ->> 'participant_id')::int AS team_id,

			(formation ->> 'formation') AS formation

		FROM src

		)

	SELECT * from flattened