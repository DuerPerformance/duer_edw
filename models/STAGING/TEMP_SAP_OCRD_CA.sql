with cte as (select * from {{source('FIVETRAN_DB','CA_OCRD')}})
select *,'SAP' as Source,'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
from cte