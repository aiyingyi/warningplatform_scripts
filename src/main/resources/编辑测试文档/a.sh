#!/bin/bash

db=warningplatform

# 充电开始，结束时间
start_time=$1
end_time=$2
vin=$3

# 定义快充和慢充
slowcharge=0
quickcharge=1

# 定义充电状态
charge=1


sql="
with
preprocess_vehicle_data0 as
(
  select
      get_json_object(data,'$.vin') vin,
      get_json_object(data,'$.msgTime') msgTime,
      get_json_object(data,'$.chargeStatus') chargeStatus,
      get_json_object(data,'$.maxCellVoltageNum') maxCellVoltageNum,
      get_json_object(data,'$.minCellVoltageNum') minCellVoltageNum,
      get_json_object(data,'$.vehicleType') vehicleType,
      get_json_object(data,'$.enterprise') enterprise,
      get_json_object(data,'$.totalCurrent') totalCurrent,
      get_json_object(data,'$.soc') soc
  from ${db}.ods_preprocess_vehicle_data where dt>=date_format('${start_time}','yyyy-MM-dd')
  and dt<=date_format('${end_time}','yyyy-MM-dd')
  and get_json_object(data,'$.vin') = '${vin}' and get_json_object(data,'$.chargeStatus') = '${charge}'
),
preprocess_vehicle_data1 as
(
  select
    enterprise,
    vehicleType,
    vin,
    msgTime,

  from preprocess_vehicle_data0 where
)

"
hive -e  "${sql}"








