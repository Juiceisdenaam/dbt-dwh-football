select
type_id,
code as type_code,
name as type_name,
model_type,
stat_group,
developer_name
from {{ ref('stg_types') }}