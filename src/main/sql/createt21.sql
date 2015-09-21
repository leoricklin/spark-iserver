use db;
create table if not exists complex_record_stag (
 report_time BIGINT
,agent_id BIGINT
,category TINYINT
,item_name STRING
,usage DOUBLE
,cdate BIGINT
,ftime TINYINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;