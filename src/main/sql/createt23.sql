use db;
create table if not exists complex_record_sum_hr (
 agent_id BIGINT
,category TINYINT
,item_name STRING
,usage_min DOUBLE
,usage_max DOUBLE
,usage_avg DOUBLE
,usage_cnt BIGINT
,aggregate_id BIGINT
,ftime TINYINT
)
PARTITIONED BY (cdate BIGINT)
STORED AS PARQUET;