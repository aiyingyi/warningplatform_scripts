-- noinspection SqlNoDataSourceInspection

create external table exception_test_es
(
    province string,
	vehicle_type  string,
	cellvol_diff_exception
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
