import datetime
import pandas as pd


def scoring_segment(reserach_period=90, currdate=pd.to_datetime(datetime.datetime.now()).date().strftime('%Y-%m-%d'),
                    seed=42):
    # table_name = 'BIG_DATA_LTV_ONLINE_OFFLINE_NEW'
    table_name = 'BIG_DATA_LTV_ONLINE_OFFLINE_NEW2025'

    query_base = f'''
with cte as (
select 
	CUSTOMER_ID 
	, CASSTICKID 
	, FIRSTORDERDATE 
	, min(CREATED_AT_ORD) as min_data_purches
	, max(CREATED_AT_ORD) as max_data_purches
	, min(TRADE_DT)
	, date_diff('day', FIRSTORDERDATE,toDate(yesterday())) as b
from 
	{table_name}
where
	FIRSTORDERDATE <= TRADE_DT 
	and date_diff('day', toDate(FIRSTORDERDATE), toDate(yesterday())) <= 90 
	and CUSTOMER_ID not in (
		select 
			CUSTOMER_ID 
		from (
			select 
				CUSTOMER_ID
				, FIRSTORDERDATE
				, min(TRADE_DT) as min_trade_dt
			from 
				{table_name}
			group by 
				CUSTOMER_ID, FIRSTORDERDATE
			  )
		where 
			FIRSTORDERDATE < min_trade_dt
	 )
group by
	CUSTOMER_ID
	, CASSTICKID 
	, FIRSTORDERDATE
),
cte_two as (
select 
	*
from 
	{table_name}
where
	CUSTOMER_ID IN (select CUSTOMER_ID from cte)
),
cte_five as (
select 
	*,
	 if(IDENTIFICATION == 'ONLINE', 1, 2) as IDENTIFICATION_INDEX,
	anyLast(FIRSTORDERDATE) over (partition by CUSTOMER_ID order by TRADE_DT) as anyLastNull

from cte_two
where CUSTOMER_ID not in (SELECT CUSTOMER_ID FROM cte_two where PRODUCT_CODE is null)
order by TRADE_DT
),
cte_mart_count as (
select CUSTOMER_ID
	, CASSTICKID
	, FIRSTORDERDATE
	, TRADE_DT
	, REGION_NAME_EN
	, USER_TYPE
	, USER_LEVEL
	, IDENTIFICATION
	, MART_NAME_RU
	, COUNT(*) as mart_count
from 
	cte_five
group by
CUSTOMER_ID
	, CASSTICKID
	, FIRSTORDERDATE
	, TRADE_DT
	, REGION_NAME_EN
	, USER_TYPE
	, USER_LEVEL
	, IDENTIFICATION
	, MART_NAME_RU
),
cte_mart_count_main as (
select *,
ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID
	, IDENTIFICATION
    , CASSTICKID
	order by mart_count DESC, cityHash64(toString(MART_NAME_RU), {seed})) as rn_mart
from cte_mart_count
),
cte_segment_count as (
select CUSTOMER_ID
	, CASSTICKID
	, FIRSTORDERDATE
	, TRADE_DT
	, REGION_NAME_EN
	, USER_TYPE
	, USER_LEVEL
	, IDENTIFICATION
	, SEGMENT_NAME_RU
	, COUNT(*) as segment_count
from 
	cte_five
group by
CUSTOMER_ID
	, CASSTICKID
	, FIRSTORDERDATE
	, TRADE_DT
	, REGION_NAME_EN
	, USER_TYPE
	, USER_LEVEL
	, IDENTIFICATION
	, SEGMENT_NAME_RU
),
cte_segment_count_main as (
select *,
ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID
	, IDENTIFICATION
    , CASSTICKID
	order by segment_count DESC, cityHash64(toString(SEGMENT_NAME_RU), {seed})) as rn_segment
from cte_segment_count
),
cte_category_count as (
select CUSTOMER_ID
	, CASSTICKID
	, FIRSTORDERDATE
	, TRADE_DT
	, REGION_NAME_EN
	, USER_TYPE
	, USER_LEVEL
	, IDENTIFICATION
	, CATEGORY_NAME_RU
	, COUNT(*) as category_count
from 
	cte_five
group by
CUSTOMER_ID
	, CASSTICKID
	, FIRSTORDERDATE
	, TRADE_DT
	, REGION_NAME_EN
	, USER_TYPE
	, USER_LEVEL
	, IDENTIFICATION
	, CATEGORY_NAME_RU
),
cte_category_count_main as (
select *,
ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID
	, IDENTIFICATION
    , CASSTICKID
	order by category_count DESC, cityHash64(toString(CATEGORY_NAME_RU), {seed})) as rn_category
from cte_category_count
),
cte_family_count as (
select CUSTOMER_ID
	, CASSTICKID
	, FIRSTORDERDATE
	, TRADE_DT
	, REGION_NAME_EN
	, USER_TYPE
	, USER_LEVEL
	, IDENTIFICATION
	, FAMILY_NAME_RU
	, COUNT(*) as family_count
from 
	cte_five
group by
CUSTOMER_ID
	, CASSTICKID
	, FIRSTORDERDATE
	, TRADE_DT
	, REGION_NAME_EN
	, USER_TYPE
	, USER_LEVEL
	, IDENTIFICATION
	, FAMILY_NAME_RU
),
cte_family_count_main as (
select *,
ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID
	, IDENTIFICATION
    , CASSTICKID
	order by family_count DESC, cityHash64(toString(FAMILY_NAME_RU), {seed})) as rn_family
from cte_family_count
),
cte_six as (
select 
	CUSTOMER_ID
	, CASSTICKID
	, anyLastNull as FIRSTORDERDATE
	, TRADE_DT
	, REGION_NAME_EN
	, USER_TYPE
	, USER_LEVEL
	, IDENTIFICATION
	, IDENTIFICATION_INDEX
	, round(min(PRICE),3) as PRICEmin
	, round(max(PRICE),3) as PRICEmax
	, round(avg(PRICE),3) as PRICEmean
	, round(sum(PRICE),3) as PRICEsum
	, coalesce(round((PRICEsum - lagInFrame(PRICEsum) OVER (PARTITION BY CUSTOMER_ID ORDER BY TRADE_DT ASC,IDENTIFICATION_INDEX ASC, sum(PRICE) DESC, CASSTICKID ASC)) /
      	lagInFrame(PRICEsum) OVER (PARTITION BY CUSTOMER_ID ORDER BY TRADE_DT ASC ,IDENTIFICATION_INDEX ASC, sum(PRICE) DESC, CASSTICKID ASC) * 100,3),0) AS PRICEpct_change
    , round(sum(PRICEsum) over (PARTITION BY CUSTOMER_ID ORDER BY TRADE_DT ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW),3) as PRICEcumsum
    , round(avg(PRICEsum) over (PARTITION BY CUSTOMER_ID ORDER BY TRADE_DT ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW),3) as PRICEexpanding_sum_mean
    , round(avg(PRICEmean) over (PARTITION BY CUSTOMER_ID ORDER BY TRADE_DT ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW),3) as PRICEexpanding_mean_mean
	, count(distinct PRODUCT_CODE) as PRODUCT_CODEnunique
	, count(PRODUCT_CODE) as PRODUCT_CODEcount
	, coalesce(dateDiff('day',lagInFrame(toNullable(TRADE_DT),1 ) OVER (PARTITION BY CUSTOMER_ID ORDER BY TRADE_DT), TRADE_DT),0) as order_diff
	, dateDiff('day', FIRSTORDERDATE, TRADE_DT) as order_diff_cum
	, coalesce(round((PRICEsum - first_value(PRICEsum) over (PARTITION BY CUSTOMER_ID ORDER BY TRADE_DT)) / first_value(PRICEsum) over (PARTITION BY CUSTOMER_ID ORDER BY TRADE_DT) * 100,3),0) as pct_change_base
	, if (lagInFrame(toNullable(REGION_NAME_EN),1 ) OVER (PARTITION BY CUSTOMER_ID ORDER BY TRADE_DT) is null, 0 , (if(REGION_NAME_EN = lagInFrame(toNullable(REGION_NAME_EN),1 ) OVER (PARTITION BY CUSTOMER_ID ORDER BY TRADE_DT),0,1))) as region_changing
from 
	cte_five
where 
	CUSTOMER_ID not in (select CUSTOMER_ID from cte_five where anyLastNull is null)
group by
	CUSTOMER_ID
	, CASSTICKID
	, FIRSTORDERDATE
	, TRADE_DT
	, REGION_NAME_EN
	, USER_TYPE
	, USER_LEVEL
	, IDENTIFICATION
	, IDENTIFICATION_INDEX
),cte_main as (
select 
	CUSTOMER_ID
	, CASSTICKID
	, FIRSTORDERDATE
	, today() - FIRSTORDERDATE as LIFETIME_DAY
	, TRADE_DT
	, IDENTIFICATION
	, IDENTIFICATION_INDEX
	, USER_TYPE
	, REGION_NAME_EN
	, USER_LEVEL
	, PRICEmax
	, PRICEmean
	, PRICEmin
	, PRICEsum
	, PRICEpct_change
	, PRICEcumsum
	, PRICEexpanding_sum_mean
	, PRICEexpanding_mean_mean
	, PRODUCT_CODEcount
	, PRODUCT_CODEnunique
	, order_diff
	, order_diff_cum
	, pct_change_base
	, any(region_changing) OVER (PARTITION BY CUSTOMER_ID, CASSTICKID ORDER BY TRADE_DT) as region_changing
from 
	cte_six
ORDER BY TRADE_DT ASC,IDENTIFICATION_INDEX ASC, PRICEsum DESC, CASSTICKID ASC
)
select cm.CUSTOMER_ID as CUSTOMER_ID
	, cm.CASSTICKID as CASSTICKID
	, cm.FIRSTORDERDATE as FIRSTORDERDATE
	, cm.LIFETIME_DAY as LIFETIME_DAY
	, cm.TRADE_DT as TRADE_DT
	, cm.IDENTIFICATION as IDENTIFICATION
	, cm.IDENTIFICATION_INDEX as IDENTIFICATION_INDEX
	, cm.USER_TYPE as USER_TYPE
	, cm.REGION_NAME_EN as REGION_NAME_EN
	, cm.USER_LEVEL as USER_LEVEL
	, cm.PRICEmax as PRICEmax
	, cm.PRICEmean as PRICEmean
	, cm.PRICEmin as PRICEmin
	, cm.PRICEsum as PRICEsum
	, cm.PRICEpct_change as PRICEpct_change
	, cm.PRICEcumsum as PRICEcumsum
	, cm.PRICEexpanding_sum_mean as PRICEexpanding_sum_mean
	, cm.PRICEexpanding_mean_mean as PRICEexpanding_mean_mean
	, cm.PRODUCT_CODEcount as PRODUCT_CODEcount
	, cm.PRODUCT_CODEnunique as PRODUCT_CODEnunique
	, cm.order_diff as order_diff
	, cm.order_diff_cum as order_diff_cum
	, cm.pct_change_base as pct_change_base
	, cm.region_changing as region_changing
    , MART_NAME_RU as MART_NAME_RUmost_frequent_category
    , SEGMENT_NAME_RU AS SEGMENT_NAME_RUmost_frequent_category
    , CATEGORY_NAME_RU AS CATEGORY_NAME_RUmost_frequent_category
    , FAMILY_NAME_RU AS FAMILY_NAME_RUmost_frequent_category
from cte_main cm
LEFT JOIN cte_mart_count_main mart_count 
ON mart_count.CUSTOMER_ID = cm.CUSTOMER_ID AND mart_count.CASSTICKID = cm.CASSTICKID AND mart_count.IDENTIFICATION = cm.IDENTIFICATION
LEFT JOIN cte_segment_count_main segment_count 
ON segment_count.CUSTOMER_ID = cm.CUSTOMER_ID AND segment_count.CASSTICKID = cm.CASSTICKID AND segment_count.IDENTIFICATION = cm.IDENTIFICATION
LEFT JOIN cte_category_count_main category_count 
ON category_count.CUSTOMER_ID = cm.CUSTOMER_ID AND category_count.CASSTICKID = cm.CASSTICKID AND category_count.IDENTIFICATION = cm.IDENTIFICATION
LEFT JOIN cte_family_count_main family_count 
ON family_count.CUSTOMER_ID = cm.CUSTOMER_ID AND family_count.CASSTICKID = cm.CASSTICKID AND family_count.IDENTIFICATION = cm.IDENTIFICATION
where rn_mart = 1  and rn_segment = 1 and rn_category = 1 and rn_family = 1

    '''

    return query_base
