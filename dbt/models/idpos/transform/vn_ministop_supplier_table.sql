WITH latest_execute_records AS (
    SELECT
        id,
        MAX(execute_target_date) AS target_date
    FROM
        {{ source('raw_stock', 'vendor') }}
    GROUP BY
        id
),

latest_supplier AS (
    SELECT
        v.id,
        v.execute_target_date,
        MAX(v.updated_at) AS latest_updated_at
    FROM
        {{ source('raw_stock', 'vendor') }} AS v
    INNER JOIN latest_execute_records
        ON
            v.id = latest_execute_records.id
            AND v.execute_target_date = latest_execute_records.target_date
    GROUP BY
        v.id,
        v.execute_target_date
)

SELECT
    v.id,
    v.name,
    NULLIF(v.business_centre, '') AS business_centre,
    NULLIF(v.contact_person, '') AS contact_person,
    NULLIF(v.address, '') AS address,
    NULLIF(v.zipcode, '') AS zipcode,
    NULLIF(v.payment_type, '') AS payment_type,
    NULLIF(v.payment_term, '') AS payment_term,
    NULLIF(v.bank_account, '') AS bank_account,
    NULLIF(v.bank_name, '') AS bank_name,
    NULLIF(v.branch_name, '') AS branch_name,
    NULLIF(v.bank_account_name, '') AS bank_account_name,
    NULLIF(v.corporate_tax_no, '') AS corporate_tax_no,
    NULLIF(v.transfer_type, '') AS transfer_type,
    FALSE AS is_deleted,
    v.execute_target_date,
    v.updated_at
FROM
    {{ source('raw_stock', 'vendor') }} AS v
INNER JOIN latest_supplier
    ON
        v.id = latest_supplier.id
        AND v.execute_target_date = latest_supplier.execute_target_date
        AND v.updated_at = latest_supplier.latest_updated_at
