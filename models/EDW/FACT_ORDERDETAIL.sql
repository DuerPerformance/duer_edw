With temp as (
select a.NAME, b.INDEX, b.PRICE, b.NAME as NAME2, sum(coalesce(c.subtotal,0)) as refundamount, sum(coalesce(c.total_tax,0)) as refundtax, CASE WHEN b.NAME LIKE '%Gift Card%' THEN 'Y' ELSE 'N' END AS ISGIFTCARD, a.Source_region, sum(coalesce(c.quantity,0)) as refund_quantity
from {{ref('SHOPIFY_ORDER')}} a left JOIN {{ref('SHOPIFY_ORDER_LINE')}} b on a.ID = b.ORDER_ID AND a.SOURCE_REGION = b.SOURCE_REGION
left join {{ref('SHOPIFY_ORDER_LINE_REFUND')}} c on b.id = c.order_line_id AND b.SOURCE_REGION = c.SOURCE_REGION
group by a.NAME, b.INDEX, b.PRICE, b.NAME, a.Source_region
order by a.name,b.index 
),
temp2 as (select
ROW_NUMBER() over (partition by order_key,"linenum" order by 1) as rn,
d.ORDER_KEY,
a."linenum" as LINE_NUM,
e.date_key_2 as order_date_key,
f.Product_Key,
g.warehouse_key,
a."price" as Price,
a."quantity" as Quantity,
a."delivrdqty" as delivered_quantity,
a."openqty" as open_quantity,
a."pricebefdi" as Pricebefdi,
a."linetotal" as Linetotal,
a."vatsum" as vatsum,
a."gtotal" as Gross_Total,
(a."price"*a."delivrdqty") as Price_Delivered,
(a."pricebefdi"*a."quantity")-(a."price"*a."quantity") as Product_Discount,
a."discprcnt" as Product_discount_Percentage,
CASE when a."discprcnt" = 100 then 'Y' Else 'N'END as ISPROMOITEM,
a."vatprcnt" as Vat_Percentage,
coalesce(temp.refundamount,0) as refund,
coalesce(temp.refundtax,0) as refund_tax,
coalesce(temp.refund_quantity,0) as refund_quantity,
coalesce(temp.isgiftcard,'N') as ISGIFTCARD,
a."linestatus" as Line_Status,
a."u_v33_cr" as Order_Line_Reason_Canceled,
a.Source, a.Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
from {{ref('SAP_RDR1')}} a
LEFT join {{ref('SAP_ORDR')}} b on a."docentry" = b."docentry" AND a.source_region = b.source_region
LEFT JOIN temp on b."numatcard" = temp.NAME AND a."linenum"+1 = temp.Index
LEFT JOIN {{ref('DIM_CALENDAR')}} e on cast(a."docdate" as DATE) = e.DATE AND e.calendar_type = 'Retail Calendar'
left JOIN {{ref('DIM_PRODUCT')}} f on a."itemcode" = f.product_id
left JOIN {{ref('DIM_ORDER')}} d on a."docentry" = d.docentry AND a.SOURCE_REGION = d.source_region
left JOIN {{ref('DIM_WAREHOUSE')}} g on a."whscode" = g.warehouse_id and a.SOURCE_REGION = g.SOURCE_REGION)
select * exclude rn from temp2 where rn = 1