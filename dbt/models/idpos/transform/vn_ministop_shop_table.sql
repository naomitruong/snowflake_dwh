WITH latest_execute_records AS (
    SELECT
        id,
        MAX(execute_target_date) AS target_date
    FROM
        {{ source('raw_stock', 'shop') }}
    WHERE id != '0'
    GROUP BY
        id
),

latest_shop AS (
    SELECT
        s.id,
        s.execute_target_date,
        MAX(s.updated_at) AS latest_updated_at
    FROM
        {{ source('raw_stock', 'shop') }} AS s
    INNER JOIN latest_execute_records
        ON
            s.id = latest_execute_records.id
            AND s.execute_target_date = latest_execute_records.target_date
    WHERE s.id != '0'
    GROUP BY
        s.id,
        s.execute_target_date
)

SELECT
    s.id,
    s.name,
    CAST(NULL AS STRING) AS zipcode,
    s.address,
    CAST(NULL AS STRING) AS prefecture_id,
    CAST(NULL AS STRING) AS prefecture_name,
    CAST(NULL AS STRING) AS phone_number,
    CAST(NULL AS STRING) AS company_id,
    CAST(NULL AS STRING) AS main_administration_id,
    NULLIF(CAST(s.priority_area AS STRING), '') AS main_administration_name,
    CAST(NULL AS STRING) AS administration_id,
    CAST(NULL AS STRING) AS administration_name,
    CAST(NULL AS NUMBER(38,0)) AS retail_l1_area,
    NULLIF(CAST(s.area AS STRING), '') AS retail_l1_area_name,
    CAST(NULL AS NUMBER(38,0)) AS retail_l2_area,
    CAST(NULL AS STRING) AS retail_l2_area_name,
    CAST(NULL AS NUMBER(38,0)) AS retail_type,
    NULLIF(CAST(s.store_type_id2 AS STRING), '') AS retail_type_name,
    CAST(NULL AS NUMBER(38,0)) AS retail_phase,
    CAST(NULL AS STRING) AS retail_phase_name,
    CAST(NULL AS NUMBER(38,0)) AS fez_l1_area,
    CAST(NULL AS NUMBER(38,0)) AS fez_l2_area,
    CAST(NULL AS NUMBER(38,0)) AS fez_type,
    CAST(NULL AS NUMBER(38,0)) AS fez_phase,
    s.open_date,
    s.close_date,
    NULLIF(s.business_centre, '') AS business_centre,
    NULLIF(s.petty_cash, '') AS petty_cash,
    NULLIF(s.store_type_id, '') AS store_type_id,
    s.renovation_date,
    NULLIF(s.frontage_m, '') AS frontage_m,
    NULLIF(s."35_MILLION", '') AS million_35,
    NULLIF(s.floor_store, '') AS floor_store,
    NULLIF(s.m2_gf, '') AS m2_gf,
    NULLIF(s.upstair_eat_in_area, '') AS upstair_eat_in_area,
    NULLIF(s.outside_eat_in_area, '') AS outside_eat_in_area,
    NULLIF(s.office_upstair, '') AS office_upstair,
    NULLIF(s.ttl, '') AS ttl,
    NULLIF(s.gon, '') AS gon,
    NULLIF(s.gon_hinght, '') AS gon_hinght,
    NULLIF(s.upright_freezer, '') AS upright_freezer,
    NULLIF(s.drink, '') AS dring,
    NULLIF(s.delivery_sf, '') AS delivery_sf,
    NULLIF(s.delivery_grab, '') AS delivery_grab,
    NULLIF(s.free_flag, '') AS free_flag,
    FALSE AS is_deleted,
    s.execute_target_date,
    s.updated_at
FROM
    {{ source('raw_stock', 'shop') }} AS s
INNER JOIN latest_shop
    ON
        s.id = latest_shop.id
        AND s.execute_target_date = latest_shop.execute_target_date
        AND s.updated_at = latest_shop.latest_updated_at
