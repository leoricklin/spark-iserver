use db;
set COMPRESSION_CODEC=snappy;
insert into basic_record_sum_hr (
 agent_id
,cpu_usage_min
,cpu_usage_max
,cpu_usage_avg
,cpu_usage_cnt
,mem_phy_usage_min
,mem_phy_usage_max
,mem_phy_usage_avg
,mem_phy_usage_cnt
,mem_cache_usage_min
,mem_cache_usage_max
,mem_cache_usage_avg
,mem_cache_usage_cnt
,mem_load_min
,mem_load_max
,mem_load_avg
,mem_load_cnt
,net_out_min
,net_out_max
,net_out_avg
,net_out_cnt
,net_in_min
,net_in_max
,net_in_avg
,net_in_cnt
,net_pkt_send_err_min
,net_pkt_send_err_max
,net_pkt_send_err_avg
,net_pkt_send_err_cnt
,net_pkt_recv_err_min
,net_pkt_recv_err_max
,net_pkt_recv_err_avg
,net_pkt_recv_err_cnt
,aggregate_id
,ftime
)
 partition (cdate)
 select
 agent_id
, min(cpu_usage)
, max(cpu_usage)
, avg(cpu_usage)
, count(cpu_usage)
, min(mem_phy_usage)
, max(mem_phy_usage)
, avg(mem_phy_usage)
, count(mem_phy_usage)
, min(mem_cache_usage)
, max(mem_cache_usage)
, avg(mem_cache_usage)
, count(mem_cache_usage)
, min(mem_load)
, max(mem_load)
, avg(mem_load)
, count(mem_load)
, min(net_out)
, max(net_out)
, avg(net_out)
, count(net_out)
, min(net_in)
, max(net_in)
, avg(net_in)
, count(net_in)
, min(net_pkt_send_err)
, max(net_pkt_send_err)
, avg(net_pkt_send_err)
, count(net_pkt_send_err)
, min(net_pkt_recv_err)
, max(net_pkt_recv_err)
, avg(net_pkt_recv_err)
, count(net_pkt_recv_err)
, cast(concat(cast(cdate as STRING) , lpad(cast(ftime as STRING), 2, '0')) as BIGINT)
, ftime
, cdate
 from basic_record
 where cdate = cast(concat(cast(extract(now(), "year") as string),cast(extract(now(), "month") as string),cast(extract(now(), "day") as string)) as bigint)
 and ftime = cast(extract(now(), "hour") as tinyint)
 group by agent_id, cdate, ftime;
set COMPRESSION_CODEC=NONE;
/*
Inserted 2341 row(s) in 1.87s
 */