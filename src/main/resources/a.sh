#!/bin/bash

db=warningplatform
# 获取当前日期
do_date=`date  "+%Y-%m-%d %H:%M:%S"`

sql="
with
risk_stat as
(
    select
      risk_level,
      count(*) total
    from ${db}.battery_warning_info_es
    where review_status = '1' or review_status = '2' or review_status = '3'
    group by risk_level
),
tmp as
(
    select
        risk_stat.risk_level,
        case risk_stat.risk_level when '1' then risk_stat.total else 0 end r1,
        case risk_stat.risk_level when '2' then risk_stat.total else 0 end r2,
        case risk_stat.risk_level when '3' then risk_stat.total else 0 end r3
    from risk_stat
)
insert into table ${db}.risk_level_statistic_es
select
    sum(tmp.r1),
    sum(tmp.r2),
    sum(tmp.r3),
    date_format('${do_date}','yyyy-MM-dd HH:mm:ss')
from tmp
"
hive -e  "${sql}"

