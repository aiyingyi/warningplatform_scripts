#!/bin/bash

db=warningplatform

# 获取当前时间
end_time=`date "+%Y-%m-%d %H:%M:%S"`
start_time=`date -d "1 hour ago" "+%Y-%m-%d %H:%M:%S"`
partition_year=`date -d "${start_time}" "+%Y"` 


# 缺少从es导入数据阶段

# 使用动态分区，按照省份和预警类型计算过去一小时的次数，按天进行分区写入到统计表中
sql="
insert overwrite table ${db}.warning_info_statistic partition(year=${partition_year},month,day)
select  tmp.*,
		date_format('${start_time}','yyyy-MM-dd HH') dt,
		date_format('${start_time}','MM') month,
		date_format('${start_time}','dd') day		
from(
	select 
		province,
		warning_type,
		count(*) total
	from ${db}.battery_warning_info 
	where date_format(dt,'yyyy-MM-dd HH') >= date_format('${start_time}','yyyy-MM-dd HH')
	and   date_format(dt,'yyyy-MM-dd HH') <  date_format('${end_time}','yyyy-MM-dd HH')
	group by province,warning_type) as tmp;
	

insert into table warning_info_statistic_es_perhour select province,warning_type,total, dt  
from  warning_info_statistic;
"

hive -e  "${sql}"





