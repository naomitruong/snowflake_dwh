WITH latest_execute_records AS (
    SELECT
        id,
        MAX(execute_target_date) AS target_date
    FROM
        {{ source('raw_stock', 'category') }}
    GROUP BY
        id
),

latest_records AS (
    SELECT
        c.id,
        c.execute_target_date,
        MAX(c.updated_at) AS latest_updated_at
    FROM
        {{ source('raw_stock', 'category') }} AS c
    INNER JOIN latest_execute_records
        ON
            c.id = latest_execute_records.id
            AND c.execute_target_date = latest_execute_records.target_date
    GROUP BY
        c.id,
        c.execute_target_date
)

SELECT
    c.id,
    c.name,
    CASE
        WHEN LENGTH(c.id) = 2 THEN 'l1'
        WHEN LENGTH(c.id) = 4 THEN 'l2'
        WHEN LENGTH(c.id) = 6 THEN 'l3'
    END AS layer,
    CASE
        WHEN LENGTH(c.id) = 2 THEN NULL
        WHEN LENGTH(c.id) = 4 THEN SUBSTR(c.id, 1, 2)
        WHEN LENGTH(c.id) = 6 THEN SUBSTR(c.id, 1, 4)
    END AS parent_id,
    FALSE AS is_deleted,
    c.execute_target_date,
    c.updated_at
FROM
    {{ source('raw_stock', 'category') }} AS c
INNER JOIN latest_records
    ON
        c.id = latest_records.id
        AND c.execute_target_date = latest_records.execute_target_date
        AND c.updated_at = latest_records.latest_updated_at
