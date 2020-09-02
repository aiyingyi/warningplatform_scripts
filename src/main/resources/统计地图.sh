#!/bin/bash

db=warningplatform
# 获取前6小时时间
do_date=`date  "+%Y-%m-%d %H:%M:%S"`
start_time=`date -d "6 hour ago  ${do_date}" "+%Y-%m-%d %H:%M:%S"`

sql="
select


from
    (select
      province,
      risk_level,
      count(*) total
    from ${db}.battery_warning_info_es
    where date_format(warning_start_time,'yyyy-MM-dd HH') >= date_format('${start_time}','yyyy-MM-dd HH')
    and date_format(warning_start_time,'yyyy-MM-dd HH') < date_format('${do_date}','yyyy-MM-dd HH')
    and review_status >= '1'
    and review_status >= '3') warning
group by
;
"
hive -e  "${sql}"

