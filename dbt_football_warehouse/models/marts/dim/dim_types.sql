SELECT
        type_id,
        type_code,
        type_name,
        model_type,
        stat_group,
        developer_name
FROM {{ ref('stg_types') }}