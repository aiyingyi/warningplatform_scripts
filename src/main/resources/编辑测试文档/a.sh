#!/bin/bash


# named_struct 会把 整数直接当成int类型，从而不能插入double类型中

db=warningplatform

coefficient=1.5

# 获取当前日期
do_date=`date  "+%Y-%m-%d %H:%M:%S"`


# 计算之前应该导入前一天的数据到dwd_preprocess_vehicle_data中
# 首先执行 ods_to_dwd_preprocess.sh 脚本，将前一天的数据导入到dwd层
sql="

insert into table  ${db}.avg_vehicle_data_perweek
 select
      province,
      vehicleType,
      vin,
      round(avg(differenceCellVoltage),1)  diff_Voltage,
      round(avg(maxProbeTemperature-minProbeTemperature),1) diff_temper,
      round(avg(maxTemperatureRate),1) temper_rate,
      round(avg(averageProbeTemperature),1) temper,
      round(avg(resistance),1) resistance,
      round(avg(wDischargeRate),1) wDischargeRate
  from ${db}.dwd_preprocess_vehicle_data
  where year >= date_format(date_add(next_day('${do_date}','MO'),-14),'yyyy')
  and year <= date_format(date_add(next_day('${do_date}','SU'),-7),'yyyy')
  and date_format(msgTime,'yyyy-MM-dd') >= date_format(date_add(next_day('${do_date}','MO'),-14),'yyyy-MM-dd')
  and date_format(msgTime,'yyyy-MM-dd') <= date_format(date_add(next_day('${do_date}','SU'),-7),'yyyy-MM-dd')
  group by province,vehicleType,vin;

with
avg_data_rank as
(
  select
    province,
    vehicleType,
    diff_Voltage,
    lead(diff_Voltage,1, 0) over(partition by province,vehicleType order by diff_Voltage)   next_diff_vol,
    row_number() over(partition by province,vehicleType order by diff_Voltage)   Vol_rk,
    temper_rate,
    lead(temper_rate,1, 0) over(partition by province,vehicleType order by temper_rate)   next_temper_rate,
    row_number() over(partition by province,vehicleType order by temper_rate)   temper_rate_rk,
    temper,
    lead(temper,1, 0) over(partition by province,vehicleType order by temper)   next_temper,
    row_number() over(partition by province,vehicleType order by temper)   temper_rk,
    diff_temper,
    lead(diff_temper,1, 0) over(partition by province,vehicleType order by diff_temper)   next_diff_temper,
    row_number() over(partition by province,vehicleType order by diff_temper)   diff_temper_rk,
    resistance,
    lead(resistance,1, 0) over(partition by province,vehicleType order by resistance)   next_resistance,
    row_number() over(partition by province,vehicleType order by resistance)   resistance_rk,
    wDischargeRate,
    lead(wDischargeRate,1, 0) over(partition by province,vehicleType order by wDischargeRate)   next_wDischargeRate,
    row_number() over(partition by province,vehicleType order by wDischargeRate)   wDischargeRate_rk
  from  ${db}.avg_vehicle_data_perweek
),
particle_location as
(
    select
        province,
        vehicleType,
        cast(3*(count(*) + 1) / 4 as int) as q3_int_part,
        (count(*)+1)*3 / 4 % 1            as q3_float_part,
        cast((count(*) + 1) / 2 as int)   as q2_int_part,
        (count(*)+1) / 2 % 1              as q2_float_part,
        cast((count(*) + 1) / 4 as int)   as q1_int_part,
        (count(*)+1) / 4 % 1              as q1_float_part
    from  avg_data_rank
    group by  province,vehicleType
),
diff_Voltage_info as
(
    select
        q3.province,
        q3.vehicleType,
        q3.vol_q3,
        q2.vol_q2,
        q1.vol_q1,
        (q3.vol_q3 + q1.vol_q1*${coefficient}) as vol_max_value,
        (q3.vol_q3 - q1.vol_q1*${coefficient}) as vol_min_value
    from
          (select
            adr.province,
            adr.vehicleType,
            adr.diff_Voltage *(1-pl.q3_float_part) + adr.next_diff_vol * pl.q3_float_part as vol_q3
          from avg_data_rank adr join particle_location pl
          on adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.Vol_rk = pl.q3_int_part) q3
    join
          (select
            adr.province,
            adr.vehicleType,
            adr.diff_Voltage *(1-pl.q2_float_part) + adr.next_diff_vol * pl.q2_float_part as vol_q2
          from avg_data_rank adr join particle_location pl
          on adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.Vol_rk = pl.q2_int_part) q2
    on q3.province = q2.province and q3.vehicleType = q2.vehicleType
    join
           (select
            adr.province,
            adr.vehicleType,
            adr.diff_Voltage *(1-pl.q1_float_part) + adr.next_diff_vol * pl.q1_float_part as vol_q1
          from avg_data_rank adr join particle_location pl
          on adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.Vol_rk = pl.q1_int_part) q1
    on q3.province = q1.province and q3.vehicleType = q1.vehicleType
)

select * from diff_Voltage_info;
"
hive -e  "${sql}"











