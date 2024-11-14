
WITH RepOrder_Data AS (

select 

distinct 

fc.ORDER_KEY,
fc.LOCATION_KEY,
fc.ORDERDATEKEY,
fc.DELIVERYDATEKEY,
fc.CANCELDATEKEY,
fc.CURRENCY_KEY,
fc.ISREJECTED,
fc.CURRENCY,
fc.ORDERPRICE,
fc.ORDERNETPRICE,
fc.ORDERDISCOUNT,
fc.ORDERTAX,
fc.ORDEREXPENDITURE,
fc.ORDERSTATUS,
fc.SOURCE_REGION,

dc.CALENDAR_TYPE	as	ORDERDATE_CALENDAR_TYPE	,
dc.DATE as ORDER_DATE_temp,
dc.DAY_NAME	as	ORDERDATE_DAY_NAME	,
dc.DATE_SEQUENCE	as	ORDERDATE_DATE_SEQUENCE	,
dc.DATE_OF_YEAR	as	ORDERDATE_DATE_OF_YEAR	,
dc.DATE_OF_QUARTER	as	ORDERDATE_DATE_OF_QUARTER	,
dc.DATE_OF_MONTH	as	ORDERDATE_DATE_OF_MONTH	,
dc.DATE_OF_WEEK	as	ORDERDATE_DATE_OF_WEEK	,
dc.YEAR_NAME	as	ORDERDATE_YEAR_NAME	,
dc.YEAR_SERIAL	as	ORDERDATE_YEAR_SERIAL	,
dc.YEAR_SEQUENCE	as	ORDERDATE_YEAR_SEQUENCE	,
dc.QUARTER_NAME	as	ORDERDATE_QUARTER_NAME	,
dc.QUARTER_SERIAL	as	ORDERDATE_QUARTER_SERIAL	,
dc.QUARTER_SEQUENCE	as	ORDERDATE_QUARTER_SEQUENCE	,
dc.QUARTER_OF_YEAR	as	ORDERDATE_QUARTER_OF_YEAR	,
dc.MONTH_NAME	as	ORDERDATE_MONTH_NAME	,
dc.MONTH_SERIAL	as	ORDERDATE_MONTH_SERIAL	,
dc.MONTH_SEQUENCE	as	ORDERDATE_MONTH_SEQUENCE	,
dc.MONTH_OF_YEAR	as	ORDERDATE_MONTH_OF_YEAR	,
dc.MONTH_OF_QUARTER	as	ORDERDATE_MONTH_OF_QUARTER	,
dc.WEEK_NAME	as	ORDERDATE_WEEK_NAME	,
dc.WEEK_SERIAL	as	ORDERDATE_WEEK_SERIAL	,
dc.WEEK_SEQUENCE	as	ORDERDATE_WEEK_SEQUENCE	,
dc.WEEK_OF_YEAR	as	ORDERDATE_WEEK_OF_YEAR	,
dc.WEEK_OF_QUARTER	as	ORDERDATE_WEEK_OF_QUARTER	,
dc.WEEK_OF_MONTH	as	ORDERDATE_WEEK_OF_MONTH,
dd.CALENDAR_TYPE	as	DELIVERYDATE_CALENDAR_TYPE	,
dd.DAY_NAME	as	DELIVERYDATE_DAY_NAME	,
dd.DATE_SEQUENCE	as	DELIVERYDATE_DATE_SEQUENCE	,
dd.DATE_OF_YEAR	as	DELIVERYDATE_DATE_OF_YEAR	,
dd.DATE_OF_QUARTER	as	DELIVERYDATE_DATE_OF_QUARTER	,
dd.DATE_OF_MONTH	as	DELIVERYDATE_DATE_OF_MONTH	,
dd.DATE_OF_WEEK	as	DELIVERYDATE_DATE_OF_WEEK	,
dd.YEAR_NAME	as	DELIVERYDATE_YEAR_NAME	,
dd.YEAR_SERIAL	as	DELIVERYDATE_YEAR_SERIAL	,
dd.YEAR_SEQUENCE	as	DELIVERYDATE_YEAR_SEQUENCE	,
dd.QUARTER_NAME	as	DELIVERYDATE_QUARTER_NAME	,
dd.QUARTER_SERIAL	as	DELIVERYDATE_QUARTER_SERIAL	,
dd.QUARTER_SEQUENCE	as	DELIVERYDATE_QUARTER_SEQUENCE	,
dd.QUARTER_OF_YEAR	as	DELIVERYDATE_QUARTER_OF_YEAR	,
dd.MONTH_NAME	as	DELIVERYDATE_MONTH_NAME	,
dd.MONTH_SERIAL	as	DELIVERYDATE_MONTH_SERIAL	,
dd.MONTH_SEQUENCE	as	DELIVERYDATE_MONTH_SEQUENCE	,
dd.MONTH_OF_YEAR	as	DELIVERYDATE_MONTH_OF_YEAR	,
dd.MONTH_OF_QUARTER	as	DELIVERYDATE_MONTH_OF_QUARTER	,
dd.WEEK_NAME	as	DELIVERYDATE_WEEK_NAME	,
dd.WEEK_SERIAL	as	DELIVERYDATE_WEEK_SERIAL	,
dd.WEEK_SEQUENCE	as	DELIVERYDATE_WEEK_SEQUENCE	,
dd.WEEK_OF_YEAR	as	DELIVERYDATE_WEEK_OF_YEAR	,
dd.WEEK_OF_QUARTER	as	DELIVERYDATE_WEEK_OF_QUARTER	,
dd.WEEK_OF_MONTH	as	DELIVERYDATE_WEEK_OF_MONTH,
cd.CALENDAR_TYPE	as	CANCELDATE_CALENDAR_TYPE	,
cd.DAY_NAME	as	CANCELDATE_DAY_NAME	,
cd.DATE_SEQUENCE	as	CANCELDATE_DATE_SEQUENCE	,
cd.DATE_OF_YEAR	as	CANCELDATE_DATE_OF_YEAR	,
cd.DATE_OF_QUARTER	as	CANCELDATE_DATE_OF_QUARTER	,
cd.DATE_OF_MONTH	as	CANCELDATE_DATE_OF_MONTH	,
cd.DATE_OF_WEEK	as	CANCELDATE_DATE_OF_WEEK	,
cd.YEAR_NAME	as	CANCELDATE_YEAR_NAME	,
cd.YEAR_SERIAL	as	CANCELDATE_YEAR_SERIAL	,
cd.YEAR_SEQUENCE	as	CANCELDATE_YEAR_SEQUENCE	,
cd.QUARTER_NAME	as	CANCELDATE_QUARTER_NAME	,
cd.QUARTER_SERIAL	as	CANCELDATE_QUARTER_SERIAL	,
cd.QUARTER_SEQUENCE	as	CANCELDATE_QUARTER_SEQUENCE	,
cd.QUARTER_OF_YEAR	as	CANCELDATE_QUARTER_OF_YEAR	,
cd.MONTH_NAME	as	CANCELDATE_MONTH_NAME	,
cd.MONTH_SERIAL	as	CANCELDATE_MONTH_SERIAL	,
cd.MONTH_SEQUENCE	as	CANCELDATE_MONTH_SEQUENCE	,
cd.MONTH_OF_YEAR	as	CANCELDATE_MONTH_OF_YEAR	,
cd.MONTH_OF_QUARTER	as	CANCELDATE_MONTH_OF_QUARTER	,
cd.WEEK_NAME	as	CANCELDATE_WEEK_NAME	,
cd.WEEK_SERIAL	as	CANCELDATE_WEEK_SERIAL	,
cd.WEEK_SEQUENCE	as	CANCELDATE_WEEK_SEQUENCE	,
cd.WEEK_OF_YEAR	as	CANCELDATE_WEEK_OF_YEAR	,
cd.WEEK_OF_QUARTER	as	CANCELDATE_WEEK_OF_QUARTER	,
cd.WEEK_OF_MONTH	as	CANCELDATE_WEEK_OF_MONTH,
do.DOCENTRY,
do.DOCNUMBER,
do.ORDERNUMBER,
do.ISCANCELED,
l.location_id,
l.location_name,
l.channel,
c.CURRENCY_ID,
c.CURRENCY_CODE,
c.CURRENCY_RATE,
c.CURRENCY_DATE,
c.CURRENCY_NAME,

coalesce(ortt."rate",1) as rate,

s.SUBSIDIARY_ID,
s.NAME as S_NAME,
s.CURRENCY as S_CURRENCY,
s.PARENT_ID,
s.SUBSIDIARYL0,
s.SUBSIDIARYL1,
s.SUBSIDIARYL2,
s.SUBSIDIARYL3,



from {{ref('FACT_ORDER')}} fc

left join {{ref('DIM_CALENDAR')}} dc 
on fc.orderdatekey=dc.date_key_2 and upper(dc.calendar_type)='Retail Calendar'

left join {{ref('DIM_CALENDAR')}} dd 
on fc.deliverydatekey=dd.date_key_2 and upper(dd.calendar_type)='Retail Calendar'

left join {{ref('DIM_CALENDAR')}} cd
on fc.canceldatekey=cd.date_key_2 and upper(cd.calendar_type)='Retail Calendar'

left join {{ref('DIM_ORDER')}} do
on fc.order_key=do.order_key

left join {{ref('DIM_LOCATION')}} l
on fc.location_key=l.location_key

left join {{ref('DIM_CURRENCY')}} c
on fc.currency_key=c.currency_key

left join {{source('FIVETRAN_DB','ORTT')}} ortt on c.currency_code = ortt."currency" and c.currency_date = ortt."ratedate"

left join {{ref('DIM_SUBSIDIARY')}} s on fc.SUBSIDIARY_KEY = s.SUBSIDIARY_KEY

)


select *, case when Source_Region = 'CA' then currency_rate else rate end as currency_rate_2 from RepOrder_Data