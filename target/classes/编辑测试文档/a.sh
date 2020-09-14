#!/bin/bash


db=warningplatform

# 获取当前日期,当前日期应该是本月最后一天
do_date=`date  "+%Y-%m-%d %H:%M:%S"`
last_month=`date  -d "${do_date} 1 month ago" "+%Y-%m"`   8月份
first_day_this_month=`date "+%Y-%m-01"`
# 获取上个月最后一天的日期
start_time=`date -d "${first_day_this_month}  last day"   "+%Y-%m-%d"`  8月31

sql="
with



"
hive -e  "${sql}"








