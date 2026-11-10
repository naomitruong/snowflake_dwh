SELECT
    id,
    name,
    business_centre,
    contact_person,
    address,
    zipcode,
    payment_type,
    payment_term,
    corporate_tax_no,
    transfer_type,
    is_deleted
FROM {{ ref('vn_ministop_supplier_table') }}
