use db;
create table if not exists basic_record_sum_hr (
 agent_id BIGINT
,cpu_usage_min Double
,cpu_usage_max Double
,cpu_usage_avg Double
,cpu_usage_cnt BIGINT
,mem_phy_usage_min Double
,mem_phy_usage_max Double
,mem_phy_usage_avg Double
,mem_phy_usage_cnt BIGINT
,mem_cache_usage_min Double
,mem_cache_usage_max Double
,mem_cache_usage_avg Double
,mem_cache_usage_cnt BIGINT
,mem_load_min Double
,mem_load_max Double
,mem_load_avg Double
,mem_load_cnt BIGINT
,net_out_min Double
,net_out_max Double
,net_out_avg Double
,net_out_cnt BIGINT
,net_in_min Double
,net_in_max Double
,net_in_avg Double
,net_in_cnt BIGINT
,net_pkt_send_err_min Double
,net_pkt_send_err_max Double
,net_pkt_send_err_avg Double
,net_pkt_send_err_cnt BIGINT
,net_pkt_recv_err_min Double
,net_pkt_recv_err_max Double
,net_pkt_recv_err_avg Double
,net_pkt_recv_err_cnt BIGINT
,aggregate_id BIGINT
,ftime TINYINT
)
PARTITIONED BY (cdate BIGINT)
 STORED AS PARQUET;
