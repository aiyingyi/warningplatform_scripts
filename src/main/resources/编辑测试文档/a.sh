#!/bin/bash


db=warningplatform
# 充电开始，结束时间
start_time=$1
end_time=$2
vin=$3


sql="
with
preprocess_vehicle_data as
(
  select
      get_json_object(data,'$.vin'),
      get_json_object(data,'$.msgTime'),
      get_json_object(data,'$.startupStatus'),
      get_json_object(data,'$.odo'),
      get_json_object(data,'$.gearStatus'),
      get_json_object(data,'$.chargeStatus'),
      get_json_object(data,'$.maxCellVoltageNum'),
      get_json_object(data,'$.maxCellVoltage'),
      get_json_object(data,'$.minCellVoltageNum'),
      get_json_object(data,'$.minCellVoltage'),
      get_json_object(data,'$.maxProbeTemperatureNum'),
      get_json_object(data,'$.maxProbeTemperature'),
      get_json_object(data,'$.minProbeTemperatureNum'),
      get_json_object(data,'$.minProbeTemperature'),
      get_json_object(data,'$.cellVoltage'),
      get_json_object(data,'$.differenceCellVoltage'),
      get_json_object(data,'$.maxTemperatureRate'),
      get_json_object(data,'$.temperatureRate'),
      get_json_object(data,'$.atanMaxTemperatureRate'),
      get_json_object(data,'$.atanMinTemperatureRate'),
      get_json_object(data,'$.averageProbeTemperature'),
      get_json_object(data,'$.averageCellVoltage'),
      get_json_object(data,'$.varianceCellVoltage'),
      get_json_object(data,'$.varianceProbeTemperature'),
      get_json_object(data,'$.entropy'),
      get_json_object(data,'$.variation'),
      get_json_object(data,'$.wDifferenceCellVoltages'),
      get_json_object(data,'$.wDifferenceTotalCellVoltage'),
      get_json_object(data,'$.differenceInternalResistance'),
      get_json_object(data,'$.averageModuleCellVoltages'),
      get_json_object(data,'$.maxModuleCellVoltages'),
      get_json_object(data,'$.minModuleCellVoltages'),
      get_json_object(data,'$.maxModuleCellVoltageNums'),
      get_json_object(data,'$.minModuleCellVoltageNums'),
      get_json_object(data,'$.totalModuleCellVoltages'),
      get_json_object(data,'$.differenceModuleCellVoltages'),
      get_json_object(data,'$.instantaneousConsumption'),
      get_json_object(data,'$.wDischargeRate'),
      get_json_object(data,'$.resistance'),
      get_json_object(data,'$.province'),
      get_json_object(data,'$.city'),
      get_json_object(data,'$.country'),
      get_json_object(data,'$.vehicleType'),
      get_json_object(data,'$.enterprise'),
      get_json_object(data,'$.totalCurrent'),
      get_json_object(data,'$.soc')
  from ${db}.ods_preprocess_vehicle_data
  where dt = '${do_date}';
)



"
hive -e  "${sql}"








