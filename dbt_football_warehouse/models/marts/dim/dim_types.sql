select
type_id,
code,
name,
model_type,
stat_group,
developer_name
from {{ ref('stg_types') }}