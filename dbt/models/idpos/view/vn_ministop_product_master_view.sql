SELECT
    p.jancode,
    p.name AS product_name,
    p.l1_category_id,
    c1.name AS l1_category_name,
    p.l2_category_id,
    c2.name AS l2_category_name,
    p.l3_category_id,
    c3.name AS l3_category_name,
    p.l4_category_id,
    CAST(NULL AS STRING) AS l4_category_name,
    CAST(NULL AS NUMBER(38, 0)) AS brand_id,
    CAST(NULL AS STRING) AS brand_name,
    CAST(NULL AS NUMBER(38, 0)) AS sub_brand_id,
    CAST(NULL AS STRING) AS sub_brand_name,
    CAST(NULL AS NUMBER(38, 0)) AS series_id,
    CAST(NULL AS STRING) AS series_name,
    CAST(NULL AS STRING) AS urumo_brand_id,
    CAST(NULL AS STRING) AS urumo_brand_name,
    p.supplier_id,
    s.name AS supplier_name,
    p.is_maker_share_excluded
FROM {{ ref('vn_ministop_product_table') }} AS p
LEFT JOIN {{ ref('vn_ministop_category_table') }} AS c1
    ON
        p.l1_category_id = c1.id
        AND c1.layer = 'l1'
LEFT JOIN {{ ref('vn_ministop_category_table') }} AS c2
    ON
        p.l2_category_id = c2.id
        AND c2.layer = 'l2'
LEFT JOIN {{ ref('vn_ministop_category_table') }} AS c3
    ON
        p.l3_category_id = c3.id
        AND c3.layer = 'l3'
LEFT JOIN {{ ref('vn_ministop_supplier_table') }} AS s
    ON p.supplier_id = s.id
