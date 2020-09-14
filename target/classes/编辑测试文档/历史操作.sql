 vol_diff_exception
 temper_rate_exception
 temper_exception
 temper_diff_exception
 resistance_exception
 discharge_rate_exception

insert into  table batterypack_exception_es
select
'山东',
'ER001',
named_struct('Q3',1.5 ,'Q2',1.0,'Q1',0.8,'maxvalue',2.7,'minvalue',0.3,"vehicles",array(named_struct('vin','1515457','outliers',3.7)),named_struct('vin','1515458','outliers',2.9))),
named_struct('Q3',15.0 ,'Q2',10.0 ,'Q1',8.0,'maxvalue',27.0,'minvalue',3.0,"vehicles",array(named_struct('vin','1515457','outliers',17.0)),named_struct('vin','1515458','outliers',16.5))),
named_struct('Q3',50.0 ,'Q2',20.0 ,'Q1',30.0,'maxvalue',95.0,'minvalue',5.0,"vehicles",array(named_struct('vin','1515457','outliers',101.2)),named_struct('vin','1515458','outliers',109.2))),
named_struct('Q3',50.0 ,'Q2',20.0 ,'Q1',30.0,'maxvalue',95.0,'minvalue',5.0,"vehicles",array(named_struct('vin','1515457','outliers',101.2)),named_struct('vin','1515458','outliers',109.2))),
named_struct('Q3',2000.0 ,'Q2',1500.0 ,'Q1',1000.0,'maxvalue',3500.0,'minvalue',500.0,"vehicles",array(named_struct('vin','1515457','outliers',4000.0)),named_struct('vin','1515458','outliers',3225.0))),
named_struct('Q3',1.5 ,'Q2',1.0,'Q1',0.8,'maxvalue',2.7,'minvalue',0.3,"vehicles",array(named_struct('vin','1515457','outliers',3.7)),named_struct('vin','1515458','outliers',2.9))),
'2020-08-31';