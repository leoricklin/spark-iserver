use db;
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
 from complex_record
 where cdate = 20150913
 and ftime = 0
 and agent_id = 6804
 group by agent_id, category, item_name, cdate, ftime;
/*
+----------+----------+-----------+------------+------------+-------------------+--------------+------------------------------------------------------------------------------------+-------+
| agent_id | category | item_name | min(usage) | max(usage) | avg(usage)        | count(usage) | cast(concat(cast(cdate as string), lpad(cast(ftime as string),2, '0')) as bigint) | ftime |
+----------+----------+-----------+------------+------------+-------------------+--------------+------------------------------------------------------------------------------------+-------+
| 6804     | 3        | /boot     | 12.4577    | 12.4577    | 12.4577           | 12           | 2015091300                                                                         | 0     |
| 6804     | 3        | /         | 1.885802   | 1.885803   | 1.885802166666667 | 12           | 2015091300                                                                         | 0     |
| 6804     | 3        | /var      | 1.317246   | 1.317246   | 1.317246          | 12           | 2015091300                                                                         | 0     |
| 6804     | 1        | sda       | 3          | 6          | 3.5               | 12           | 2015091300                                                                         | 0     |
+----------+----------+-----------+------------+------------+-------------------+--------------+------------------------------------------------------------------------------------+-------+

 */