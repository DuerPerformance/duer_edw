WITH cte AS (
    SELECT DISTINCT COALESCE("country", 'N/A') AS Country 
    FROM {{ref('SAP_CRD1')}}	
    UNION
    SELECT DISTINCT COALESCE("countryorg", 'N/A') AS Country 
    FROM {{ref('SAP_RDR1')}}
    UNION
    SELECT DISTINCT COALESCE("countryorg", 'N/A') AS Country 
    FROM {{ref('SAP_OITM')}}
),
-- Add surrogate key using dbt's `surrogate_key` macro
countryKey_with_keys AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY Country) AS countryKey,
        Country,
        'SAP' AS Source,  -- Hardcoded
        CURRENT_TIMESTAMP() AS InsertDate,  -- Auto-generated
        CURRENT_TIMESTAMP() AS UpdateDate,  -- Auto-generated
        'I' AS DML  -- Hardcoded
    FROM cte
    WHERE Country IS NOT NULL AND Country <> ''
)

SELECT * 
FROM countryKey_with_keys