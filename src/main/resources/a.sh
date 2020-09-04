#!/bin/bash

# named_struct 会把 整数直接当成int类型，从而不能插入double类型中

db=warningplatform
# 获取当前日期
do_date=`date  "+%Y-%m-%d %H:%M:%S"`

sql="
with
    as

"
hive -e  "${sql}"

