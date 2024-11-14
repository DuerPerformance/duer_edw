select a.* exclude (SOURCE, INSERT_DATE, UPDATE_DATE, DML),
b.* exclude (order_key, SOURCE, INSERT_DATE, UPDATEDATE, DML) RENAME SOURCE_REGION as SOURCE_REGION_order,
c.*,
d.* exclude (product_key, INSERTDATE,UPDATEDATE, DML),
e.* exclude (warehouse_key, SOURCE, INSERTDATE, UPDATEDATE, DML) RENAME SOURCE_REGION as SOURCE_REGION_wh
from {{ref('FACT_ORDERDETAIL')}} a
left join {{ref('DIM_ORDER')}} b on a.order_key = b.order_key
left join {{ref('DIM_CALENDAR')}} c on a.order_date_key = c.date_key_2 and c.calendar_type = 'Retail Calendar'
left join {{ref('DIM_PRODUCT')}} d on a.product_key = d.product_key
left join {{ref('DIM_WAREHOUSE')}} E on a.WAREHOUSE_key = E.WAREHOUSE_key