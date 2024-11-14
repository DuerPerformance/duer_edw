with cte as
(
    select distinct COALESCE("cityb",'N/A') AS City from {{ref('SAP_RDR12')}}
    union
    select distinct COALESCE("citys",'N/A') AS City from {{ref('SAP_RDR12')}}
    union
    select distinct COALESCE("SHIPPING_ADDRESS_CITY",'N/A') AS City from  {{ref('SHOPIFY_ORDER')}} --shopify data
    union
    select distinct COALESCE("BILLING_ADDRESS_CITY",'N/A') AS City from  {{ref('SHOPIFY_ORDER')}} --shopify data
    union
    select distinct COALESCE("city",'N/A') AS City from   {{ref('SAP_OWHS')}}	
),
-- Add surrogate key using dbt's `surrogate_key` macro
cityKey_with_keys AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY City) AS cityKey,
        City,
        'SAP & shopify' AS Source,  -- Hardcoded
        CURRENT_TIMESTAMP() AS InsertDate,  -- Auto-generated
        CURRENT_TIMESTAMP() AS UpdateDate,  -- Auto-generated
        'I' AS DML  -- Hardcoded
    FROM cte
    WHERE City IS NOT NULL AND City <> ''
)

SELECT * 
FROM cityKey_with_keys