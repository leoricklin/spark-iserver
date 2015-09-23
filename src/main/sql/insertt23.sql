use db;
set COMPRESSION_CODEC=snappy;
insert into complex_record_sum_hr (
 agent_id
,category
,item_name
,usage_min
,usage_max
,usage_avg
,usage_cnt
,aggregate_id
,ftime
)
 partition (cdate)
 select
 agent_id
,category
,item_name
,min(usage)
,max(usage)
,avg(usage)
,count(usage)
, cast(concat(cast(cdate as STRING) , lpad(cast(ftime as STRING), 2, '0')) as BIGINT)
,ftime
,cdate
 from complex_record
 where cdate = 20150913
 and ftime = 0
 group by agent_id, category, item_name, cdate, ftime;
set COMPRESSION_CODEC=NONE;
/*
select * from complex_record_sum_hr where cdate = 20150913 and agent_id = 6804;
+----------+----------+-----------+-----------+-----------+-------------------+-----------+--------------+-------+----------+
| agent_id | category | item_name | usage_min | usage_max | usage_avg         | usage_cnt | aggregate_id | ftime | cdate    |
+----------+----------+-----------+-----------+-----------+-------------------+-----------+--------------+-------+----------+
| 6804     | 3        | /boot     | 12.4577   | 12.4577   | 12.4577           | 12        | 2015091300   | 0     | 20150913 |
| 6804     | 3        | /         | 1.885802  | 1.885803  | 1.885802166666667 | 12        | 2015091300   | 0     | 20150913 |
| 6804     | 3        | /var      | 1.317246  | 1.317246  | 1.317246          | 12        | 2015091300   | 0     | 20150913 |
| 6804     | 1        | sda       | 3         | 6         | 3.5               | 12        | 2015091300   | 0     | 20150913 |
+----------+----------+-----------+-----------+-----------+-------------------+-----------+--------------+-------+----------+
 */