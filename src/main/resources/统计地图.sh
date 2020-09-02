#!/bin/bash

db=warningplatform
# 获取前6小时时间
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
    and review_status >= '1'
    and review_status >= '3'
    group by province,risk_level ),
high_risk as
(select province,total from warning where risk_level = '1'),
med_risk as
(select province,total from warning where risk_level = '2'),
low_risk as
(select province,total from warning where risk_level = '3')

insert into table province_warning_statistic_es
select
  high_risk.province,
  high_risk.total,
  med_risk.total,
  low_risk.total,
  0,
  date_format('${start_time}','yyyy-MM-dd HH')
from
  high_risk join  med_risk on med_risk.province = high_risk.province
join low_risk on high_risk.province = low_risk.province;
"
hive -e  "${sql}"

