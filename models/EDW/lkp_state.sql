with cte as
(
    select distinct COALESCE("stateb",'N/A') AS State from {{ref('SAP_RDR12')}}
    union
    select distinct COALESCE("states",'N/A') AS State from {{ref('SAP_RDR12')}}
    union
    select distinct COALESCE("SHIPPING_ADDRESS_PROVINCE_CODE",'N/A') AS State from  {{ref('SHOPIFY_ORDER')}} --shopify data 
    union
    select distinct COALESCE("BILLING_ADDRESS_PROVINCE_CODE",'N/A') AS State from  {{ref('SHOPIFY_ORDER')}} --shopify data
    union
    select distinct COALESCE("state",'N/A') AS State from   {{ref('SAP_OWHS')}}	
),
-- Add surrogate key using dbt's `surrogate_key` macro
stateKey_with_keys AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY State) AS stateKey,
        State,
        'SAP & Shopify' AS Source,  -- Hardcoded
        CURRENT_TIMESTAMP() AS InsertDate,  -- Auto-generated
        CURRENT_TIMESTAMP() AS UpdateDate,  -- Auto-generated
        'I' AS DML  -- Hardcoded
    FROM cte
    WHERE state IS NOT NULL AND State <>''
)

SELECT * 
FROM stateKey_with_keys