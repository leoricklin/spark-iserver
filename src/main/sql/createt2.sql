use db;
create table if not exists complex_record (
 report_time BIGINT
,agent_id BIGINT
,category TINYINT
,item_name STRING
,usage DOUBLE
)
PARTITIONED BY (cdate BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;