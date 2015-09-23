use db;
select report_time, agent_id, cpu_usage
 , mem_phy_usage, mem_cache_usage, mem_load
 , net_out, net_in, net_pkt_send_err, net_pkt_recv_err, cdate, ftime
 from basic_record
 where
 report_time = 1441872159969
 and agent_id = 31208;
/*
+---------------+----------+-----------+---------------+-----------------+-----------+---------+--------+------------------+------------------+----------+-------+
| report_time   | agent_id | cpu_usage | mem_phy_usage | mem_cache_usage | mem_load  | net_out | net_in | net_pkt_send_err | net_pkt_recv_err | cdate    | ftime |
+---------------+----------+-----------+---------------+-----------------+-----------+---------+--------+------------------+------------------+----------+-------+
| 1441872159969 | 31208    | 0.098273  | 61.927822     | 0.043043        | 40.852562 | 3       | 303    | 0                | 0                | 20150921 | 13    |
+---------------+----------+-----------+---------------+-----------------+-----------+---------+--------+------------------+------------------+----------+-------+
Fetched 1 row(s) in 5.28s
Fetched 1 row(s) in 2.38s
Fetched 1 row(s) in 3.37s
 */
 select report_time, agent_id, cpu_usage
  , mem_phy_usage, mem_cache_usage, mem_load
  , net_out, net_in, net_pkt_send_err, net_pkt_recv_err, cdate, ftime
  from basic_record
  where cdate = 20150921 and ftime = 13
  and report_time = 1441872159969 and agent_id = 31208;
/*
Fetched 1 row(s) in 0.51s
Fetched 1 row(s) in 0.14s
Fetched 1 row(s) in 0.13s

> select agent_id, min(cpu_usage), max(cpu_usage), avg(cpu_usage), count(cpu_usage) from basic_record
where cdate = 20150913 and ftime = 0 and agent_id in (6804,13884)
group by agent_id, cdate, ftime;
+----------+----------------+----------------+----------------+------------------+
| agent_id | min(cpu_usage) | max(cpu_usage) | avg(cpu_usage) | count(cpu_usage) |
+----------+----------------+----------------+----------------+------------------+
| 6804     | 0.054636       | 0.178218       | 0.077935       | 12               |
| 13884    | 0              | 0              | 0              | 12               |
+----------+----------------+----------------+----------------+------------------+
Fetched 2 row(s) in 1.01s
 */

