use db;
select * from basic_record
 where cdate=20150914
 and report_time = 1441872159969
 and agent_id = 31208;
/*
>  select * from basic_record limit 1;
+---------------------------+------------------------+-------------------------+-----------------------------+-------------------------------+------------------------+-----------------------+----------------------+--------------------------------+--------------------------------+---------------------+--+
| basic_record.report_time  | basic_record.agent_id  | basic_record.cpu_usage  | basic_record.mem_phy_usage  | basic_record.mem_cache_usage  | basic_record.mem_load  | basic_record.net_out  | basic_record.net_in  | basic_record.net_pkt_send_err  | basic_record.net_pkt_recv_err  | basic_record.cdate  |
+---------------------------+------------------------+-------------------------+-----------------------------+-------------------------------+------------------------+-----------------------+----------------------+--------------------------------+--------------------------------+---------------------+--+
| NULL                      | NULL                   | 0.360151                | 26.021778                   | 13.62933                      | 18.107843              | 0.0                   | 1.0                  | 0.0                            | 0.0                            | 20150914            |
+---------------------------+------------------------+-------------------------+-----------------------------+-------------------------------+------------------------+-----------------------+----------------------+--------------------------------+--------------------------------+---------------------+--+

>  select * from basic_record where cdate=20150915 limit 1;
+---------------------------+------------------------+-------------------------+-----------------------------+-------------------------------+------------------------+-----------------------+----------------------+--------------------------------+--------------------------------+---------------------+--+
| basic_record.report_time  | basic_record.agent_id  | basic_record.cpu_usage  | basic_record.mem_phy_usage  | basic_record.mem_cache_usage  | basic_record.mem_load  | basic_record.net_out  | basic_record.net_in  | basic_record.net_pkt_send_err  | basic_record.net_pkt_recv_err  | basic_record.cdate  |
+---------------------------+------------------------+-------------------------+-----------------------------+-------------------------------+------------------------+-----------------------+----------------------+--------------------------------+--------------------------------+---------------------+--+
| NULL                      | NULL                   | 0.360151                | 26.021778                   | 13.62933                      | 18.107843              | 0.0                   | 1.0                  | 0.0                            | 0.0                            | 20150915            |
+---------------------------+------------------------+-------------------------+-----------------------------+-------------------------------+------------------------+-----------------------+----------------------+--------------------------------+--------------------------------+---------------------+--+

 */