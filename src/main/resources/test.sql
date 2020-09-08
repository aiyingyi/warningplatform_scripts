
-- 插入数据
insert into  table batterypack_exception_es
select
'山东',
'ER001',
named_struct('Q3',15.0 ,'Q2',15.0 ,'Q1',15.0,'maxvalue',15.0,'minvalue',15.0,"vehicles",array(named_struct('vin','1515457','outliers',15.0))),
named_struct('Q3',15.0 ,'Q2',15.0 ,'Q1',15.0,'maxvalue',15.0,'minvalue',15.0,"vehicles",array(named_struct('vin','1515457','outliers',15.0))),
named_struct('Q3',15.0 ,'Q2',15.0 ,'Q1',15.0,'maxvalue',15.0,'minvalue',15.0,"vehicles",array(named_struct('vin','1515457','outliers',15.0))),
named_struct('Q3',15.0 ,'Q2',15.0 ,'Q1',15.0,'maxvalue',15.0,'minvalue',15.0,"vehicles",array(named_struct('vin','1515457','outliers',15.0))),
named_struct('Q3',15.0 ,'Q2',15.0 ,'Q1',15.0,'maxvalue',15.0,'minvalue',15.0,"vehicles",array(named_struct('vin','1515457','outliers',15.0))),
named_struct('Q3',15.0 ,'Q2',15.0 ,'Q1',15.0,'maxvalue',15.0,'minvalue',15.0,"vehicles",array(named_struct('vin','1515457','outliers',15.0))),
'2020-02-12';

----------------------------------------------------------------------------------------------------------
-- 2. 求箱线图数据----------------------------------------------
with  num_rank as     -- 按照id分区，分区内按照维度值进行分区，然后再查出下一列的维度值
(select
    num,
    id,
    row_number() over(partition by id order by num) as rk,
    lead(num,1, 0) over(partition by id order by num) as next_num
from test
),
Q2_location as  -- 中位数的位置
(
    select
         id,
         cast((count(num) + 1) / 2 as int) as int_part,
         (count(num)+1) / 2 % 1            as float_part
    from num_rank
    group by id
),
Q2_info as    -- 求出中位数
(
    select
    num_rank.id,
    num_rank.num *(1-Q2_location.float_part)+(num_rank.next_num) * Q2_location.float_part as q2

    from num_rank join Q2_location
    on num_rank.id = Q2_location.id  and num_rank.rk = Q2_location.int_part
),
Q1_location as  -- Q1数的位置
(
    select
         id,
         cast((count(num) + 1) / 4 as int) as int_part,
         (count(num)+1) / 4 % 1            as float_part
    from num_rank
    group by id
),
Q1_info as    -- 求出Q2数
(
    select
    num_rank.id,
    num_rank.num *(1-Q1_location.float_part)+(num_rank.next_num)*Q1_location.float_part as q1

    from num_rank join Q1_location
    on num_rank.id = Q1_location.id  and num_rank.rk = Q1_location.int_part
),
Q3_location as  -- Q3数的位置
(
    select
         id,
         cast(3*(count(num) + 1) / 4 as int) as int_part,
         (count(num)+1)*3 / 4 % 1            as float_part
    from num_rank
    group by id
),
Q3_info as    -- 求出Q3数
(
    select
    num_rank.id,
    num_rank.num *(1-Q3_location.float_part)+(num_rank.next_num)*Q3_location.float_part as q3

    from num_rank join Q3_location
    on num_rank.id = Q3_location.id  and num_rank.rk = Q3_location.int_part
),
limit_value as (
    select
        Q3_info.id,
        Q3_info.q3-Q1_info.q1*1.5 as min_value,
        Q3_info.q3+Q1_info.q1*1.5 as max_value
    from  Q1_info join  Q3_info on Q1_info.id = Q3_info.id
),
total_info as (
select
    Q1_info.id,
    Q1_info.q1,
    Q2_info.q2,
    Q3_info.q3,
    limit_value.max_value,
    limit_value.min_value
from Q1_info join  Q2_info on  Q1_info.id = Q2_info.id
join Q3_info on  Q1_info.id = Q3_info.id
join  limit_value on  limit_value.id = Q1_info.id
)
select * from total_info;

-- 测试建表-------------------------------------------------------------------------------------------
with
avg_vehicle_data_perweek   as        -- 按照省份，车型计算每辆车在一周内的各个维度的均值
(
  select
      province,
      vehicleType,
      vin,
      round(avg(differenceCellVoltage),1)  diff_Voltage,
      round(avg(maxProbeTemperature),1) diff_temper,
      round(avg(maxTemperatureRate),1) temper_rate,
      round(avg(averageProbeTemperature),1) temper,
      round(avg(resistance),1) resistance,
      round(avg(wDischargeRate),2) wDischargeRate
  from test
  group by province,vehicleType,vin
),
avg_data_rank as       --  按照省份，车型，计算每一个维度数据的排名以及下一行的数值。
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
  from  avg_vehicle_data_perweek
),
particle_location as    -- 计算各个四分位数的位置
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
wDischargeRate_info as   -- 计算出温度差的箱线值
(
    select
        q3.province,
        q3.vehicleType,
        q3.wDischargeRate_q3,
        q2.wDischargeRate_q2,
        q1.wDischargeRate_q1,
        q3.wDischargeRate_q3 + q1.wDischargeRate_q1*1.5 as wDischargeRate_max_value,
        q3.wDischargeRate_q3 - q1.wDischargeRate_q1*1.5 as wDischargeRate_min_value
    from
          (select
            adr.province,
            adr.vehicleType,
            adr.wDischargeRate *(1-pl.q3_float_part) + adr.next_wDischargeRate * pl.q3_float_part as wDischargeRate_q3
          from avg_data_rank adr join particle_location pl
          on adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.wDischargeRate_rk = pl.q3_int_part) q3
    join
          (select
            adr.province,
            adr.vehicleType,
            adr.wDischargeRate *(1-pl.q2_float_part) + adr.next_wDischargeRate * pl.q2_float_part as wDischargeRate_q2
          from avg_data_rank adr join particle_location pl
          on adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.wDischargeRate_rk = pl.q2_int_part) q2
    join
          (select
            adr.province,
            adr.vehicleType,
            adr.wDischargeRate *(1-pl.q1_float_part) + adr.next_wDischargeRate * pl.q1_float_part as wDischargeRate_q1
          from avg_data_rank adr join particle_location pl
          on adr.province = pl.province and  adr.vehicleType = pl.vehicleType
          and adr.wDischargeRate_rk = pl.q1_int_part) q1
)
select * from wDischargeRate_info


drop table test;

create table test(
   province  string,
   vehicleType string,
   vin string ,
    differenceCellVoltage double,
    maxProbeTemperature double,
    maxTemperatureRate double,
    averageProbeTemperature double,
    resistance double,
    wDischargeRate double
) row format delimited fields  terminated by '\t'


insert into table test values
("山顶","ER001","aaaaa",7,7,7,7,7,7),
("山顶","ER001","aaaaa",15,15,15,15,15,15),
("山顶","ER001","aaaaa",36,36,36,36,36,36),
("山顶","ER001","aaaaa",39,39,39,39,39,39),
("山顶","ER001","aaaaa",40,40,40,40,40,40),
("山顶","ER001","aaaaa",41,41,41,41,41,41)


select * from test;


