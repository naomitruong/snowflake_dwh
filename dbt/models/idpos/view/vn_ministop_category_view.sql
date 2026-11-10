SELECT
    id,
    name,
    layer,
    parent_id,
    is_deleted
FROM {{ ref('vn_ministop_category_table') }}
