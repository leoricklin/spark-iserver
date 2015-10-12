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
 where cdate = cast(concat(cast(extract(now(), "year") as string),cast(extract(now(), "month") as string),cast(extract(now(), "day") as string)) as bigint)
 and ftime = cast(extract(now(), "hour") as tinyint)
 group by agent_id, category, item_name, cdate, ftime;
set COMPRESSION_CODEC=NONE;
