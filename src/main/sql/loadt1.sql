use db;
load data inpath '/hive/tlbd_upload/iserver/parquet/basic.20150915' into table basic_record partition (cdate=20150915);