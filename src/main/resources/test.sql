create external table batterypack_exception_es
(
    province string,
	vehicle_type  string,
	vol_diff_exception
	struct<Q3:double ,Q2:double ,Q1:double ,maxvalue:double,minvalue:double,vehicles:array<struct<vin:string,outliers:double >>>,
	temper_rate_exception
	struct<Q3:double ,Q2:double ,Q1:double ,maxvalue:double,minvalue:double,vehicles:array<struct<vin:string,outliers:double >>>,
	temper_exception
	struct<Q3:double ,Q2:double ,Q1:double ,maxvalue:double,minvalue:double,vehicles:array<struct<vin:string,outliers:double >>>,
	temper_diff_exception
	struct<Q3:double ,Q2:double ,Q1:double ,maxvalue:double,minvalue:double,vehicles:array<struct<vin:string,outliers:double >>>,
	resistance_exception
	struct<Q3:double ,Q2:double ,Q1:double ,maxvalue:double,minvalue:double,vehicles:array<struct<vin:string,outliers:double >>>,
	discharge_rate_exception
	struct<Q3:double ,Q2:double ,Q1:double ,maxvalue:double,minvalue:double,vehicles:array<struct<vin:string,outliers:double >>>,
	dt string
) row format delimited fields terminated by ','
collection items terminated by '_'
map keys terminated by ':'
 STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
 TBLPROPERTIES ('es.resource' = 'batterypack_exception/batterypack_exception',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );


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
'2020-02-12'

----------------------------------------------------------------------------------------------------------
create external table exception_test_es
(
    province string,
	vehicle_type  string,
	cellvol_diff_exception,
	struct<Q3:double ,Q2:double ,Q1:double ,maxvalue:double,minvalue:double,vehicles:array<struct<vin:string,outliers:double >>>,
	dt string
) row format delimited fields terminated by ','
collection items terminated by '_'
map keys terminated by ':'
 STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
 TBLPROPERTIES ('es.resource' = 'exception_test/exception_test',
        'es.nodes' = '192.168.11.29',
        'es.port' = '9200'
        );

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


