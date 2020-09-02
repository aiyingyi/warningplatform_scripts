#!/bin/bash

db=warningplatform

# 获取前一小时时间
do_date=`date  "+%Y-%m-%d %H:%M:%S"`
start_time=`date -d "1 hour ago ${do_date}" "+%Y-%m-%d %H:%M:%S"`
partition_year=`date -d "${start_time}" "+%Y"` 

# 1. 从es导入数据阶段

# 2. 使用动态分区，按照省份和预警类型计算过去一小时的故障次数，按天进行分区写入到统计表中
sql="
insert overwrite table ${db}.battery_warning_info
select
  vin,
  warning_type,
  province,
  date_format(warning_start_time,'yyyy-MM-dd HH') as dt
from ${db}.battery_warning_info_es where date_format(warning_start_time,'yyyy-MM-dd HH') >= date_format('${start_time}','yyyy-MM-dd HH')
and  date_format(warning_start_time,'yyyy-MM-dd HH') < date_format('${do_date}','yyyy-MM-dd HH');


insert into table ${db}.warning_info_statistic partition(year=${partition_year},month,day)
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
	where date_format(dt,'yyyy-MM-dd HH') = date_format('${start_time}','yyyy-MM-dd HH')
	group by province,warning_type) as tmp;

insert into table ${db}.warning_info_statistic_es_perhour
select province,warning_type,total, dt
from  ${db}.warning_info_statistic where year = date_format('${start_time}','yyyy')
and month =  date_format('${start_time}','MM')
and day = date_format('${start_time}','dd')
and dt = date_format('${start_time}','yyyy-MM-dd HH');
"

hive -e  "${sql}"





