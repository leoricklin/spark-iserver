use db;
create table if not exists basic_record (
 report_time BIGINT
,agent_id BIGINT
,cpu_usage DOUBLE
,mem_phy_usage DOUBLE
,mem_cache_usage DOUBLE
,mem_load DOUBLE
,net_out DOUBLE
,net_in DOUBLE
,net_pkt_send_err DOUBLE
,net_pkt_recv_err DOUBLE
)
PARTITIONED BY (cdate BIGINT)
STORED AS PARQUET;