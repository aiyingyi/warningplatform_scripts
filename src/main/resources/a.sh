#!/bin/bash

# named_struct 会把 整数直接当成int类型，从而不能插入double类型中

db=warningplatform
# 获取当前日期
do_date=`date  "+%Y-%m-%d %H:%M:%S"`


# 缺少导入数据dwd_preprocess_vehicle_data
sql="
with
avg_vehicle_data_perweek   as        -- 按照省份，车型计算每辆车在一周内的各个维度的均值
(
  select
      province,
      vehicleType,
      vin,
      round(avg(differenceCellVoltage),1)  diff_Voltage,
      round(avg(maxProbeTemperature-minProbeTemperature),1) diff_temper,
      round(avg(maxTemperatureRate),1) temperatureRate,
      round(avg(averageProbeTemperature),1) temper,
      round(avg(resistance),1) resistance,
      round(avg(wDischargeRate),2) wDischargeRate
  from ${db}.dwd_preprocess_vehicle_data
  where year >= date_format(date_add(next_day('${do_date}','MO'),-14),'yyyy')
  and year <= date_format(date_add(next_day('${do_date}','SU'),-7),'yyyy')
  and date_format(msgTime,'yyyy-MM-dd') >= date_format(date_add(next_day('${do_date}','MO'),-14),'yyyy-MM-dd')
  and date_format(msgTime,'yyyy-MM-dd') <= date_format(date_add(next_day('${do_date}','SU'),-7),'yyyy-MM-dd')
  group by province,vehicleType,vin
),
diff_CellVol_rank as     --  求压差的排名以及下一个值
(
  select
    province,
    vehicleType,
    row_number() over(partition by province,vehicleType order by diff_Voltage)   Vol_rk,
    diff_Voltage,
    lead(diff_Voltage,1, 0) over(partition by province,vehicleType order by diff_Voltage)   next_diff_vol
  from  avg_vehicle_data_perweek
)



"
hive -e  "${sql}"







