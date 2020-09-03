#!/bin/bash

db=warningplatform
# 获取前6小时时间，有可能跨天
do_date=`date  "+%Y-%m-%d %H:%M:%S"`
start_time=`date -d "6 hour ago  ${do_date}" "+%Y-%m-%d %H:%M:%S"`

sql="

with
warning as
(select
      province,
      risk_level,
      count(*) total
    from ${db}.battery_warning_info_es
    where date_format(warning_start_time,'yyyy-MM-dd HH') >= date_format('${start_time}','yyyy-MM-dd HH')
    and date_format(warning_start_time,'yyyy-MM-dd HH') < date_format('${do_date}','yyyy-MM-dd HH')
    and (review_status = '1' or review_status = '2' or review_status = '3')
    group by province,risk_level ),
tmp as
(select warning.*
  from warning
  distribute by warning.province
  sort by warning.province,warning.risk_level),

warning_level as
(select
    tmp.province,
    case tmp.risk_level when '1' then tmp.total else 0 end r1,
    case tmp.risk_level when '2' then tmp.total else 0 end r2,
    case tmp.risk_level when '3' then tmp.total else 0 end r3
from tmp
)

insert into table ${db}.province_warning_statistic_es
select
  warning_level.province,
  sum(warning_level.r1) r1,
  sum(warning_level.r2) r2,
  sum(warning_level.r3) r3,
  0,
  date_format('${start_time}','yyyy-MM-dd HH')
from warning_level
group by warning_level.province
"
hive -e  "${sql}"

