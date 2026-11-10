WITH latest_execute_records AS (
    SELECT
        id,
        MAX(execute_target_date) AS target_date
    FROM
        {{ source('raw_stock', 'product') }}
    GROUP BY
        id
),

latest_product AS (
    SELECT
        p.id,
        p.execute_target_date,
        MAX(p.updated_at) AS latest_updated_at
    FROM
        {{ source('raw_stock', 'product') }} AS p
    INNER JOIN latest_execute_records
        ON
            p.id = latest_execute_records.id
            AND p.execute_target_date = latest_execute_records.target_date
    GROUP BY
        p.id,
        p.execute_target_date
),

base AS (
    SELECT
        p.id AS jancode,
        p.id,
        p.name,
        p.storage_type AS standard,
        NULLIF(p.barcode1, '') AS barcode1,
        NULLIF(p.barcode2, '') AS barcode2,
        NULLIF(p.barcode3, '') AS barcode3,
        NULLIF(p.barcode4, '') AS barcode4,
        NULLIF(p.barcode5, '') AS barcode5,
        NULLIF(p.barcode6, '') AS barcode6,
        NULLIF(p.barcode7, '') AS barcode7,
        NULLIF(p.barcode8, '') AS barcode8,
        CASE
            WHEN p.excl_tax_hq_po_price IS NOT NULL AND p.excl_tax_hq_po_price != ''
                THEN CAST(FLOOR(TRY_CAST(p.excl_tax_hq_po_price AS FLOAT)) AS NUMBER(38, 0))
        END AS list_price,
        p.dept_id AS l1_category_id,
        p.class_id AS l2_category_id,
        p.sub_class_id AS l3_category_id,
        CAST(NULL AS STRING) AS l4_category_id,
        -- supplier_override（seed）に登録された商品は supplier_id を付け替える（#7486）
        -- One-Time Vendor に混在するメーカー特定済み商品を本来のメーカーに寄せる暫定対応
        COALESCE(override.supplier_id, p.vendor_id) AS supplier_id,
        p.vendor_id AS source_supplier_id,
        CAST(NULL AS STRING) AS brand_id,
        p.output_tax AS tax_type,
        NULLIF(p.vendor_name, '') AS vendor_name,
        CASE
            WHEN p.input_tax_rate IS NOT NULL AND p.input_tax_rate != ''
                THEN TRY_CAST(REPLACE(p.input_tax_rate, '%', '') AS NUMBER(38, 9))
        END AS input_tax_rate,
        NULLIF(p.output_tax_wholesale, '') AS outpu_tax_wholesale,
        CASE
            WHEN p.incl_tax_hq_po_price IS NOT NULL AND p.incl_tax_hq_po_price != ''
                THEN TRY_CAST(p.incl_tax_hq_po_price AS NUMBER(38, 9))
        END AS incl_tax_hq_po_price,
        CASE
            WHEN p.excl_tax_store_po_price IS NOT NULL AND p.excl_tax_store_po_price != ''
                THEN TRY_CAST(p.excl_tax_store_po_price AS NUMBER(38, 9))
        END AS excl_tax_store_po_price,
        CASE
            WHEN p.incl_tax_store_po_price IS NOT NULL AND p.incl_tax_store_po_price != ''
                THEN TRY_CAST(p.incl_tax_store_po_price AS NUMBER(38, 9))
        END AS incl_tax_store_po_price,
        CASE
            WHEN p.excl_tax_retail_price IS NOT NULL AND p.excl_tax_retail_price != ''
                THEN TRY_CAST(p.excl_tax_retail_price AS NUMBER(38, 9))
        END AS excl_tax_retail_price,
        CASE
            WHEN p.incl_tax_retail_price IS NOT NULL AND p.incl_tax_retail_price != ''
                THEN TRY_CAST(p.incl_tax_retail_price AS NUMBER(38, 9))
        END AS incl_tax_retail_price,
        CASE
            WHEN p.excl_tax_returns IS NOT NULL AND p.excl_tax_returns != ''
                THEN TRY_CAST(p.excl_tax_returns AS NUMBER(38, 9))
        END AS excl_tax_returns,
        CASE
            WHEN p.incl_tax_returns IS NOT NULL AND p.incl_tax_returns != ''
                THEN TRY_CAST(p.incl_tax_returns AS NUMBER(38, 9))
        END AS incl_tax_returns,
        CASE
            WHEN p.net_profit_rate IS NOT NULL AND p.net_profit_rate != ''
                THEN TRY_CAST(REPLACE(p.net_profit_rate, '%', '') AS NUMBER(38, 9))
        END AS net_profit_rate,
        NULLIF(p.delivery_type, '') AS delivery_type,
        CASE
            WHEN p.min_order_qty_dc IS NOT NULL AND p.min_order_qty_dc != ''
                THEN CAST(TRY_CAST(p.min_order_qty_dc AS NUMBER(38, 9)) AS NUMBER(38, 0))
        END AS min_order_qty_dc,
        CASE
            WHEN p.order_multipier_dc IS NOT NULL AND p.order_multipier_dc != ''
                THEN CAST(TRY_CAST(p.order_multipier_dc AS NUMBER(38, 9)) AS NUMBER(38, 0))
        END AS order_multipier_dc,
        CASE
            WHEN p.min_order_qty_store IS NOT NULL AND p.min_order_qty_store != ''
                THEN CAST(TRY_CAST(p.min_order_qty_store AS NUMBER(38, 9)) AS NUMBER(38, 0))
        END AS min_order_qty_store,
        CASE
            WHEN p.order_multipier_store IS NOT NULL AND p.order_multipier_store != ''
                THEN CAST(TRY_CAST(p.order_multipier_store AS NUMBER(38, 9)) AS NUMBER(38, 0))
        END AS order_multipier_store,
        NULLIF(p.product_type, '') AS product_type,
        NULLIF(p.place_of_origin, '') AS place_of_origin,
        NULLIF(p.grade, '') AS grade,
        NULLIF(p.uom, '') AS uom,
        NULLIF(p.shelf_life_type, '') AS shelf_life_type,
        NULLIF(p.std_validity, '') AS std_validity,
        NULLIF(p.dc_gr_shelf_life, '') AS dc_gr_shelf_life,
        NULLIF(p.dc_gd_shelf_life, '') AS dc_gd_shelf_life,
        NULLIF(p.store_shelf_life, '') AS store_shelf_life,
        NULLIF(p.expiry_hour, '') AS expiry_hour,
        NULLIF(p.store_cant_return, '') AS store_cant_return,
        NULLIF(p.dept_name, '') AS dept_name,
        NULLIF(p.class_name, '') AS class_name,
        NULLIF(p.sub_class_name, '') AS sub_class_name,
        FALSE AS is_deleted,
        p.execute_target_date,
        p.updated_at
    FROM {{ source('raw_stock', 'product') }} AS p
    INNER JOIN latest_product
        ON
            p.id = latest_product.id
            AND p.execute_target_date = latest_product.execute_target_date
            AND p.updated_at = latest_product.latest_updated_at
    -- 受領値が seed の source_supplier_id と一致する場合のみ上書き（受領側で修正済みなら受領値を優先）
    LEFT JOIN {{ ref('vn_ministop_supplier_override') }} AS override
        ON
            LPAD(TRIM(p.id), 13, '0') = override.jancode
            AND p.vendor_id = override.source_supplier_id
    WHERE
        p.dept_id != '60' -- Promotion Item
        AND p.dept_id != '69' -- Consignment/Services
)

SELECT
    LPAD(TRIM(jancode), 13, '0') AS jancode,
    id,
    name,
    standard,
    barcode1,
    barcode2,
    barcode3,
    barcode4,
    barcode5,
    barcode6,
    barcode7,
    barcode8,
    list_price,
    l1_category_id,
    l2_category_id,
    l3_category_id,
    l4_category_id,
    supplier_id,
    brand_id,
    tax_type,
    vendor_name,
    input_tax_rate,
    outpu_tax_wholesale,
    incl_tax_hq_po_price,
    excl_tax_store_po_price,
    incl_tax_store_po_price,
    excl_tax_retail_price,
    incl_tax_retail_price,
    excl_tax_returns,
    incl_tax_returns,
    net_profit_rate,
    delivery_type,
    min_order_qty_dc,
    order_multipier_dc,
    min_order_qty_store,
    order_multipier_store,
    product_type,
    place_of_origin,
    grade,
    uom,
    shelf_life_type,
    std_validity,
    dc_gr_shelf_life,
    dc_gd_shelf_life,
    store_shelf_life,
    expiry_hour,
    store_cant_return,
    dept_name,
    class_name,
    sub_class_name,
    is_deleted,
    FALSE AS is_maker_share_excluded,
    execute_target_date,
    updated_at
FROM base
-- 重複排除の順序は付け替え前（受領値）の supplier_id で判定し、seed の有無で選ばれる行が変わらないようにする
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LPAD(TRIM(jancode), 13, '0')
    ORDER BY
        source_supplier_id NULLS FIRST,
        list_price NULLS FIRST,
        tax_type NULLS FIRST
) = 1
