#!/bin/bash

db=warningplatform

# 获取当前时间
do_date=`date "+%Y-%m-%d %H:%M:%S"`

# 获取前一周的数据进行聚合
# dt为计算周的周一日期
sql="
select tmp.*,
       date_format(date_add(next_day('${do_date}','MO'),-14),'yyyy-MM-dd') as dt 
from	   
	(select  
		province,
		warning_type,
		sum(total) as total
	from ${db}.warning_info_statistic 
	where year >= date_format(date_add(next_day('${do_date}','MO'),-14),'yyyy')
	and   year <= date_format(date_add(next_day('${do_date}','MO'),-8),'yyyy')
	and     date_format(dt,'yyyy-MM-dd') >= date_format(date_add(next_day('${do_date}','MO'),-14),'yyyy-MM-dd')
	and     date_format(dt,'yyyy-MM-dd') <= date_format(date_add(next_day('${do_date}','MO'),-8),'yyyy-MM-dd')
	group by province,warning_type) as tmp;
"

hive -e "${sql}"
	