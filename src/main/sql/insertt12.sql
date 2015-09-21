use db;
set COMPRESSION_CODEC=snappy;
insert into basic_record (
 report_time
,agent_id
,cpu_usage
,mem_phy_usage
,mem_cache_usage
,mem_load
,net_out
,net_in
,net_pkt_send_err
,net_pkt_recv_err
)
 partition (cdate, ftime)
select
 report_time
,agent_id
,cpu_usage
,mem_phy_usage
,mem_cache_usage
,mem_load
,net_out
,net_in
,net_pkt_send_err
,net_pkt_recv_err
,cdate
,ftime
from basic_record_stag;
set COMPRESSION_CODEC=NONE;
/*
Inserted 4562093 row(s) in 9.87s

> show partitions complex_record
+----------+-------+-------+--------+-----------+--------------+---------+-------------------+
| cdate    | ftime | #Rows | #Files | Size      | Bytes Cached | Format  | Incremental stats |
+----------+-------+-------+--------+-----------+--------------+---------+-------------------+
| 20150910 | 0     | -1    | 1      | 1017.83KB | NOT CACHED   | PARQUET | false             |
| 20150910 | 1     | -1    | 1      | 1015.88KB | NOT CACHED   | PARQUET | false             |
| 20150910 | 2     | -1    | 1      | 1015.49KB | NOT CACHED   | PARQUET | false             |
| 20150910 | 3     | -1    | 1      | 1015.09KB | NOT CACHED   | PARQUET | false             |
| 20150910 | 4     | -1    | 1      | 1.01MB    | NOT CACHED   | PARQUET | false             |
...
| 20150916 | 22    | -1    | 1      | 1.06MB    | NOT CACHED   | PARQUET | false             |
| 20150916 | 23    | -1    | 1      | 1.05MB    | NOT CACHED   | PARQUET | false             |
| 20150917 | 0     | -1    | 1      | 2.06KB    | NOT CACHED   | PARQUET | false             |
 */