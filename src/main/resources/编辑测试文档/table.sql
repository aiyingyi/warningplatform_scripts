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
-- 获取车辆充电时间内的原始数据
preprocess_vehicle_data0 as
(
  select
      get_json_object(data,'$.vin') vin,
      get_json_object(data,'$.msgTime') msgTime,
      get_json_object(data,'$.maxCellVoltageNum') maxCellVoltageNum,
      get_json_object(data,'$.minCellVoltageNum') minCellVoltageNum,
      get_json_object(data,'$.vehicleType') vehicleType,
      get_json_object(data,'$.enterprise') enterprise,
      get_json_object(data,'$.totalCurrent') totalCurrent,
      get_json_object(data,'$.soc') soc
  from ${db}.ods_preprocess_vehicle_data where dt>=date_format('${start_time}','yyyy-MM-dd')
  and dt<=date_format('${end_time}','yyyy-MM-dd')      --根据日期分区查找数据
  and get_json_object(data,'$.msgTime') >= '${start_time}'
  and get_json_object(data,'$.msgTime') <= '${end_time}'
  and get_json_object(data,'$.vin') = '${vin}'
-- and get_json_object(data,'$.chargeStatus') = '${charge}'   匹配充电状态？
),
-- 计算出初始数据
preprocess_vehicle_data1 as
(
  select
    enterprise,
    vehicleType,
    vin,
    maxCellVoltageNum,
    minCellVoltageNum,
    totalCurrent,
    avg(totalCurrent) over(partition by vin) as avgCurrent,
    max(totalCurrent) over(partition by vin) as maxCurrent,
    msgTime-lag(msgTime,1,msgTime)  over(partition by vin order by msgTime asc ) as timeDiff,  -- 获取时间差,第一行取0
    min(soc) over(partition by vin) as chargeStartSOC,  -- 开始SOC
    max(soc) over(partition by vin) as chargeEndSOC     -- 结束SOC
  from preprocess_vehicle_data0
),
-- 计算出充电模式
charge_mode_electricity as
(
  select
    vin,
    case when avgCurrent>= 30  or maxCurrent >=70 then '${quickcharge}' else '${slowcharge}' end as  chargeType,
    timeDiff*totalCurrent as chargeElectricity
  from  preprocess_vehicle_data1
),

-- 计算最大电压单体频次
maxvol_cell_frequency as
(
    select
      tmp.vin,
      collect_list(
          tmp.max1,tmp.max2,tmp.max3,tmp.max4,tmp.max5,tmp.max6,tmp.max7,tmp.max8,tmp.max9,tmp.max10,
          tmp.max11,tmp.max12,tmp.max13,tmp.max14,tmp.max15,tmp.max16,tmp.max17,tmp.max18,tmp.max19,tmp.max20,
          tmp.max21,tmp.max22,tmp.max23,tmp.max24,tmp.max25,tmp.max26,tmp.max27,tmp.max28,tmp.max29,tmp.max30,
          tmp.max31,tmp.max32,tmp.max33,tmp.max34,tmp.max35,tmp.max36,tmp.max37,tmp.max38,tmp.max39,tmp.max40,
          tmp.max41,tmp.max42,tmp.max43,tmp.max44,tmp.max45,tmp.max46,tmp.max47,tmp.max48,tmp.max49,tmp.max50,
          tmp.max51,tmp.max52,tmp.max53,tmp.max54,tmp.max55,tmp.max56,tmp.max57,tmp.max58,tmp.max59,tmp.max60,
          tmp.max61,tmp.max62,tmp.max63,tmp.max64,tmp.max65,tmp.max66,tmp.max67,tmp.max68,tmp.max69,tmp.max70,
          tmp.max71,tmp.max72,tmp.max73,tmp.max74,tmp.max75,tmp.max76,tmp.max77,tmp.max78,tmp.max79,tmp.max80,
          tmp.max81, tmp.max82, tmp.max83, tmp.max84, tmp.max85, tmp.max86, tmp.max87, tmp.max88, tmp.max89, tmp.max90,
          tmp.max91,tmp.max92,tmp.max93,tmp.max94,tmp.max95,tmp.max96
      ) as max_frequency
    from (
      select
        maxvol.vin,
        sum(maxvol.max1) as max1,
        sum(maxvol.max2) as max2,
        sum(maxvol.max3) as max3,
        sum(maxvol.max4) as max4,
        sum(maxvol.max5) as max5,
        sum(maxvol.max6) as max6,
        sum(maxvol.max7) as max7,
        sum(maxvol.max8) as max8,
        sum(maxvol.max9) as max9,
        sum(maxvol.max10) as max10,
        sum(maxvol.max11) as max11,
        sum(maxvol.max12) as max12,
        sum(maxvol.max13) as max13,
        sum(maxvol.max14) as max14,
        sum(maxvol.max15) as max15,
        sum(maxvol.max16) as max16,
        sum(maxvol.max17) as max17,
        sum(maxvol.max18) as max18,
        sum(maxvol.max19) as max19,
        sum(maxvol.max20) as max20,
        sum(maxvol.max21) as max21,
        sum(maxvol.max22) as max22,
        sum(maxvol.max23) as max23,
        sum(maxvol.max24) as max24,
        sum(maxvol.max25) as max25,
        sum(maxvol.max26) as max26,
        sum(maxvol.max27) as max27,
        sum(maxvol.max28) as max28,
        sum(maxvol.max29) as max29,
        sum(maxvol.max30) as max30,
        sum(maxvol.max31) as max31,
        sum(maxvol.max32) as max32,
        sum(maxvol.max33) as max33,
        sum(maxvol.max34) as max34,
        sum(maxvol.max35) as max35,
        sum(maxvol.max36) as max36,
        sum(maxvol.max37) as max37,
        sum(maxvol.max38) as max38,
        sum(maxvol.max39) as max39,
        sum(maxvol.max40) as max40,
        sum(maxvol.max41) as max41,
        sum(maxvol.max42) as max42,
        sum(maxvol.max43) as max43,
        sum(maxvol.max44) as max44,
        sum(maxvol.max45) as max45,
        sum(maxvol.max46) as max46,
        sum(maxvol.max47) as max47,
        sum(maxvol.max48) as max48,
        sum(maxvol.max49) as max49,
        sum(maxvol.max50) as max50,
        sum(maxvol.max51) as max51,
        sum(maxvol.max52) as max52,
        sum(maxvol.max53) as max53,
        sum(maxvol.max54) as max54,
        sum(maxvol.max55) as max55,
        sum(maxvol.max56) as max56,
        sum(maxvol.max57) as max57,
        sum(maxvol.max58) as max58,
        sum(maxvol.max59) as max59,
        sum(maxvol.max60) as max60,
        sum(maxvol.max61) as max61,
        sum(maxvol.max62) as max62,
        sum(maxvol.max63) as max63,
        sum(maxvol.max64) as max64,
        sum(maxvol.max65) as max65,
        sum(maxvol.max66) as max66,
        sum(maxvol.max67) as max67,
        sum(maxvol.max68) as max68,
        sum(maxvol.max69) as max69,
        sum(maxvol.max70) as max70,
        sum(maxvol.max71) as max71,
        sum(maxvol.max72) as max72,
        sum(maxvol.max73) as max73,
        sum(maxvol.max74) as max74,
        sum(maxvol.max75) as max75,
        sum(maxvol.max76) as max76,
        sum(maxvol.max77) as max77,
        sum(maxvol.max78) as max78,
        sum(maxvol.max79) as max79,
        sum(maxvol.max80) as max80,
        sum(maxvol.max81) as max81,
        sum(maxvol.max82) as max82,
        sum(maxvol.max83) as max83,
        sum(maxvol.max84) as max84,
        sum(maxvol.max85) as max85,
        sum(maxvol.max86) as max86,
        sum(maxvol.max87) as max87,
        sum(maxvol.max88) as max88,
        sum(maxvol.max89) as max89,
        sum(maxvol.max90) as max90,
        sum(maxvol.max91) as max91,
        sum(maxvol.max92) as max92,
        sum(maxvol.max93) as max93,
        sum(maxvol.max94) as max94,
        sum(maxvol.max95) as max95,
        sum(maxvol.max96) as max96
      from
          (select
          max_vol_num.vin,
          case maxCellVoltageNum when '1' then max_vol_num.total else 0 end  as max1,
          case maxCellVoltageNum when '2' then max_vol_num.total else 0 end  as max2,
          case maxCellVoltageNum when '3' then max_vol_num.total else 0 end  as max3,
          case maxCellVoltageNum when '4' then max_vol_num.total else 0 end  as max4,
          case maxCellVoltageNum when '5' then max_vol_num.total else 0 end  as max5,
          case maxCellVoltageNum when '6' then max_vol_num.total else 0 end  as max6,
          case maxCellVoltageNum when '7' then max_vol_num.total else 0 end  as max7,
          case maxCellVoltageNum when '8' then max_vol_num.total else 0 end  as max8,
           case maxCellVoltageNum when '9' then max_vol_num.total else 0 end  as max9,
          case maxCellVoltageNum when '10' then max_vol_num.total else 0 end  as max10,
          case maxCellVoltageNum when '11' then max_vol_num.total else 0 end  as max11,
          case maxCellVoltageNum when '12' then max_vol_num.total else 0 end  as max12,
          case maxCellVoltageNum when '13' then max_vol_num.total else 0 end  as max13,
          case maxCellVoltageNum when '14' then max_vol_num.total else 0 end  as max14,
          case maxCellVoltageNum when '15' then max_vol_num.total else 0 end  as max15,
          case maxCellVoltageNum when '16' then max_vol_num.total else 0 end  as max16,
           case maxCellVoltageNum when '17' then max_vol_num.total else 0 end  as max17,
          case maxCellVoltageNum when '18' then max_vol_num.total else 0 end  as max18,
          case maxCellVoltageNum when '19' then max_vol_num.total else 0 end  as max19,
          case maxCellVoltageNum when '20' then max_vol_num.total else 0 end  as max20,
          case maxCellVoltageNum when '21' then max_vol_num.total else 0 end  as max21,
          case maxCellVoltageNum when '22' then max_vol_num.total else 0 end  as max22,
          case maxCellVoltageNum when '23' then max_vol_num.total else 0 end  as max23,
          case maxCellVoltageNum when '24' then max_vol_num.total else 0 end  as max24,
           case maxCellVoltageNum when '25' then max_vol_num.total else 0 end  as max25,
          case maxCellVoltageNum when '26' then max_vol_num.total else 0 end  as max26,
          case maxCellVoltageNum when '27' then max_vol_num.total else 0 end  as max27,
          case maxCellVoltageNum when '28' then max_vol_num.total else 0 end  as max28,
          case maxCellVoltageNum when '29' then max_vol_num.total else 0 end  as max29,
          case maxCellVoltageNum when '30' then max_vol_num.total else 0 end  as max30,
          case maxCellVoltageNum when '31' then max_vol_num.total else 0 end  as max31,
          case maxCellVoltageNum when '32' then max_vol_num.total else 0 end  as max32,
           case maxCellVoltageNum when '33' then max_vol_num.total else 0 end  as max33,
          case maxCellVoltageNum when '34' then max_vol_num.total else 0 end  as max34,
          case maxCellVoltageNum when '35' then max_vol_num.total else 0 end  as max35,
          case maxCellVoltageNum when '36' then max_vol_num.total else 0 end  as max36,
          case maxCellVoltageNum when '37' then max_vol_num.total else 0 end  as max37,
          case maxCellVoltageNum when '38' then max_vol_num.total else 0 end  as max38 ,
          case maxCellVoltageNum when '39' then max_vol_num.total else 0 end  as max39,
          case maxCellVoltageNum when '40' then max_vol_num.total else 0 end  as max40,
           case maxCellVoltageNum when '41' then max_vol_num.total else 0 end  as max41,
          case maxCellVoltageNum when '42' then max_vol_num.total else 0 end  as max42,
          case maxCellVoltageNum when '43' then max_vol_num.total else 0 end  as max43,
          case maxCellVoltageNum when '44' then max_vol_num.total else 0 end  as max44,
          case maxCellVoltageNum when '45' then max_vol_num.total else 0 end  as max45,
          case maxCellVoltageNum when '46' then max_vol_num.total else 0 end  as max46,
          case maxCellVoltageNum when '47' then max_vol_num.total else 0 end  as max47,
          case maxCellVoltageNum when '48' then max_vol_num.total else 0 end  as max48,
           case maxCellVoltageNum when '49' then max_vol_num.total else 0 end  as max49,
          case maxCellVoltageNum when '50' then max_vol_num.total else 0 end  as max50,
          case maxCellVoltageNum when '51' then max_vol_num.total else 0 end  as max51,
          case maxCellVoltageNum when '52' then max_vol_num.total else 0 end  as max52,
          case maxCellVoltageNum when '53' then max_vol_num.total else 0 end  as max53,
          case maxCellVoltageNum when '54' then max_vol_num.total else 0 end  as max54,
          case maxCellVoltageNum when '55' then max_vol_num.total else 0 end  as max55,
          case maxCellVoltageNum when '56' then max_vol_num.total else 0 end  as max56,
           case maxCellVoltageNum when '57' then max_vol_num.total else 0 end  as max57,
          case maxCellVoltageNum when '58' then max_vol_num.total else 0 end  as max58,
          case maxCellVoltageNum when '59' then max_vol_num.total else 0 end  as max59,
          case maxCellVoltageNum when '60' then max_vol_num.total else 0 end  as max60,
          case maxCellVoltageNum when '61' then max_vol_num.total else 0 end  as max61,
          case maxCellVoltageNum when '62' then max_vol_num.total else 0 end  as max62,
          case maxCellVoltageNum when '63' then max_vol_num.total else 0 end  as max63,
          case maxCellVoltageNum when '64' then max_vol_num.total else 0 end  as max64,
           case maxCellVoltageNum when '65' then max_vol_num.total else 0 end  as max65,
          case maxCellVoltageNum when '66' then max_vol_num.total else 0 end  as max66,
          case maxCellVoltageNum when '67' then max_vol_num.total else 0 end  as max67,
          case maxCellVoltageNum when '68' then max_vol_num.total else 0 end  as max68,
          case maxCellVoltageNum when '69' then max_vol_num.total else 0 end  as max69,
          case maxCellVoltageNum when '70' then max_vol_num.total else 0 end  as max70,
          case maxCellVoltageNum when '71' then max_vol_num.total else 0 end  as max71,
          case maxCellVoltageNum when '72' then max_vol_num.total else 0 end  as max72,
           case maxCellVoltageNum when '73' then max_vol_num.total else 0 end  as max73,
          case maxCellVoltageNum when '74' then max_vol_num.total else 0 end  as max74,
          case maxCellVoltageNum when '75' then max_vol_num.total else 0 end  as max75,
          case maxCellVoltageNum when '76' then max_vol_num.total else 0 end  as max76,
          case maxCellVoltageNum when '77' then max_vol_num.total else 0 end  as max77,
          case maxCellVoltageNum when '78' then max_vol_num.total else 0 end  as max78,
          case maxCellVoltageNum when '79' then max_vol_num.total else 0 end  as max79,
          case maxCellVoltageNum when '80' then max_vol_num.total else 0 end  as max80,
           case maxCellVoltageNum when '81' then max_vol_num.total else 0 end  as max81,
          case maxCellVoltageNum when '82' then max_vol_num.total else 0 end  as max82,
          case maxCellVoltageNum when '83' then max_vol_num.total else 0 end  as max83,
          case maxCellVoltageNum when '84' then max_vol_num.total else 0 end  as max84,
          case maxCellVoltageNum when '85' then max_vol_num.total else 0 end  as max85,
          case maxCellVoltageNum when '86' then max_vol_num.total else 0 end  as max86,
          case maxCellVoltageNum when '87' then max_vol_num.total else 0 end  as max87,
          case maxCellVoltageNum when '88' then max_vol_num.total else 0 end  as max88,
           case maxCellVoltageNum when '89' then max_vol_num.total else 0 end  as max89,
          case maxCellVoltageNum when '90' then max_vol_num.total else 0 end  as max90,
          case maxCellVoltageNum when '91' then max_vol_num.total else 0 end  as max91,
          case maxCellVoltageNum when '92' then max_vol_num.total else 0 end  as max92,
          case maxCellVoltageNum when '93' then max_vol_num.total else 0 end  as max93,
          case maxCellVoltageNum when '94' then max_vol_num.total else 0 end  as max94,
          case maxCellVoltageNum when '95' then max_vol_num.total else 0 end  as max95,
          case maxCellVoltageNum when '96' then max_vol_num.total else 0 end  as max96
          from
            (select
            vin,
            maxCellVoltageNum,
            count(maxCellVoltageNum) as total
            from  preprocess_vehicle_data0
            group by vin,maxCellVoltageNum) as max_vol_num ) as  maxvol
    ) as tmp
)

cell_vol_frequency as
(

)





"
hive -e  "${sql}"








