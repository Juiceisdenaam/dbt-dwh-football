SELECT
        tvstation_id,
        tvstation_name,
        tvstation_url,
        tvstation_type,
        tvstation_image_path,
        related_id
FROM
    {{ ref('stg_tv_stations') }}