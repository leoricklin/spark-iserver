use db;
create table if not exists complex_record (
 report_time BIGINT
,agent_id BIGINT
,category TINYINT
,item_name STRING
,usage DOUBLE
)
PARTITIONED BY (cdate BIGINT, ftime TINYINT)
STORED AS PARQUET;