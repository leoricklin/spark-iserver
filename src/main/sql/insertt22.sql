use db;
set COMPRESSION_CODEC=snappy;
insert into complex_record (
report_time
,agent_id
,category
,item_name
,usage
)
 partition (cdate, ftime)
select
report_time
,agent_id
,category
,item_name
,usage
,cdate
,ftime
from complex_record_stag;
set COMPRESSION_CODEC=NONE;
/*
Inserted 46388311 row(s) in 14.02s
> show partitions basic_record;
+----------+-------+-------+--------+----------+--------------+---------+-------------------+
| cdate    | ftime | #Rows | #Files | Size     | Bytes Cached | Format  | Incremental stats |
+----------+-------+-------+--------+----------+--------------+---------+-------------------+
| 20150910 | 0     | -1    | 1      | 589.89KB | NOT CACHED   | PARQUET | false             |
| 20150910 | 1     | -1    | 1      | 589.84KB | NOT CACHED   | PARQUET | false             |
| 20150910 | 2     | -1    | 1      | 587.30KB | NOT CACHED   | PARQUET | false             |
| 20150910 | 3     | -1    | 1      | 592.05KB | NOT CACHED   | PARQUET | false             |
| 20150910 | 4     | -1    | 1      | 601.98KB | NOT CACHED   | PARQUET | false             |
...
| 20150916 | 23    | -1    | 1      | 648.44KB | NOT CACHED   | PARQUET | false             |
| 20150917 | 0     | -1    | 1      | 2.24KB   | NOT CACHED   | PARQUET | false             |
 */