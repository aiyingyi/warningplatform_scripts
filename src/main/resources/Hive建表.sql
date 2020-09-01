-- 获取子字符串函数
select   substr('123456', 1 , length('123456')-1)  ;



-- 创建Hive 离线数据库

CREATE DATABASE IF NOT EXISTS warningplatform LOCATION '/warningplatform.db';

USE warningplatform;


-- 创建预警类型表
create external table warning_type(
    id int,
    type_name string,
    level_id int comment '预警等级id'
)row format delimited fields terminated by '\t'
location '/warningplatform.db/dwd/warning_type';


-- 创建故障类型表
create external table failure_type(
    id int,
    type_name string
)row format delimited fields terminated by '\t'
location '/warningplatform.db/dwd/failure_type';


-- 创建预警等级表
create external table warning_level(
    id int,
    level_type string
)row format delimited fields terminated by '\t'
location '/warningplatform.db/dwd/warning_level';


-- 创建预警信息表，每小时从es中拉取一次数据，不用分区，直接覆盖掉即可,拉取上一个小时的数据

create external table battery_warning_info(
    vin string,
    warning_type string comment '预警类型',
    province string,
    dt timestamp
)row format delimited fields terminated by '\t'
location '/warningplatform.db/dwt/battery_warning_info';

-- 创建预警信息统计表，按照小时进行统计，按照时间分区
create external table warning_info_statistic(
    province string,
    warning_type string comment '预警类型',
    total bigint  comment '故障次数',
    dt string comment '本次统计范围的开始整点'
)partitioned by (year string,month string,day string)
row format delimited fields terminated by '\t'
location '/warningplatform.db/ads/warning_info_statistic';


-- 创建预警统计信息表与es每小时统计表的映射表  
-- 注意添加hive写入es的两个jar包

CREATE EXTERNAL TABLE warning_info_statistic_es_perhour(
    province string,
    warning_type string comment '预警类型',
    total bigint  comment '故障次数',
    dt string comment '本次统计范围的开始整点'
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource'='warninginfo_statistic_perweek/warninginfo_statistic_perweek',
'es.nodes'='192.168.11.29',
'es.port'='9200',
'es.index.auto.create'='TRUE'
);



























-- 原数数据表，保存清洗之后的json数据

CREATE EXTERNAL TABLE IF NOT EXISTS  ods_vehicleInfo(
	msg string
)
partitioned by (year string, month string, day string)   
row format delimited fields terminated by '\t'
location '/warningplatform.db/ods';

-- 创建数据清洗之后表格

CREATE EXTERNAL TABLE IF NOT EXISTS  dwd_vehicleInfo(
	vin	string,
	msgTime bigint,
	speed double,
	startupStatus int,
	runMode int,
	odo double comment '累计里程',                    
	gearStatus int comment '档位',     
	chargeStatus int comment '充电状态',
	maxVCellNum int comment '最高电压电池单体代号',
	cellMaxVoltage double comment '电池单体电压最高值',
	minVCellNum int,
	cellMinVoltage double,
	maxTProbeNum int,
	probeMaxTemperature double,
	minProbeNum int,
	probeMinTemperature double,
	aptv double comment '加速踏板行程值',
	bptv int comment '制动踏板状态',
	totalVoltage double,
	totalCurrent double,
	SOC string,
	insulationResistance double,
	positionStatus int,
	longitude double,
	latitude double,
	failure string,                       
	cellNum int,                          
	probeNum int,                         
	cellVoltage array<double>,
	probeTemperature array<double>
) 
partitioned by (year string, month string, day string)   
row format delimited fields terminated by '\t'
collection items terminated by ',';

-- 插入dwd_vehicleInfo数据表
insert overwrite table dwd_vehicleinfo partition(year,month,day)
select 
    get_json_object(msg,'$.vin') vin,
    get_json_object(msg,'$.msgTime') msgTime,
    get_json_object(msg,'$.speed') speed,
    get_json_object(msg,'$.startupStatus') startupStatus,
    get_json_object(msg,'$.runMode') runMode,
    get_json_object(msg,'$.odo') odo,
    get_json_object(msg,'$.gearStatus') gearStatus,
    get_json_object(msg,'$.chargeStatus') chargeStatus,
    get_json_object(msg,'$.maxVCellNum') maxVCellNum,
    get_json_object(msg,'$.cellMaxVoltage') cellMaxVoltage,
    get_json_object(msg,'$.minVCellNum') minVCellNum,
    get_json_object(msg,'$.cellMinVoltage') cellMinVoltage,
    get_json_object(msg,'$.maxTProbeNum') maxTProbeNum,
    get_json_object(msg,'$.probeMaxTemperature') probeMaxTemperature,
    get_json_object(msg,'$.minProbeNum') minProbeNum,
    get_json_object(msg,'$.probeMinTemperature') probeMinTemperature,
    get_json_object(msg,'$.aptv') aptv,
    get_json_object(msg,'$.bptv') bptv,
    get_json_object(msg,'$.totalVoltage') totalVoltage,
    get_json_object(msg,'$.totalCurrent') totalCurrent,
    get_json_object(msg,'$.SOC') SOC,
    get_json_object(msg,'$.insulationResistance') insulationResistance,
    get_json_object(msg,'$.positionStatus') positionStatus,
    get_json_object(msg,'$.longitude') longitude,
    get_json_object(msg,'$.latitude') latitude,
    get_json_object(msg,'$.failure') failure,
    get_json_object(msg,'$.cellNum') cellNum,
    get_json_object(msg,'$.probeNum') probeNum,
    get_json_object(msg,'$.cellVoltage') cellVoltage,
    get_json_object(msg,'$.probeTemperature') probeTemperature
from ods_vehicleinfo where year = '2009' and month = '03' and day = '16';





