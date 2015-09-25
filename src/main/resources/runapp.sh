NOW=`date +%Y%m%d%H%M%S`
LOG=/home/leoricklin/log/spark-$NOW
SRC=/hive/tlbd_upload/iserver/log/small
STAG=/hive/tlbd_upload/iserver/log/$NOW
### move src files to staging directory
hdfs dfs -mkdir $STAG
hdfs dfs -mv "$SRC/*.log" $STAG
### call app to process files in staging directory
spark-submit --class tw.com.chttl.iserver.Main --master yarn-cluster --queue root.PERSONAL.leoricklin --driver-memory 8g --executor-memory 8g --executor-cores 1 --num-executors 4 --jars file:///home/leoricklin/jar/spark-util.jar --driver-class-path '/home/cloudera/parcels/CDH/lib/hive/lib/*' --driver-java-options '-Dspark.executor.extraClassPath=/home/cloudera/parcels/CDH/lib/hive/lib/*' --conf spark.driver.maxResultSize=2g /home/leoricklin/jar/spark-iserver.jar hdfs://nameservice1$STAG hdfs://nameservice1/hive/tlbd_upload/iserver/txt/basic basic_record_stag basic_record hdfs://nameservice1/hive/tlbd_upload/iserver/txt/complex complex_record_stag complex_record root.PERSONAL.leoricklin jdbc:hive2://10.176.32.44:21050 tlbd leoricklin leoricklin >> ${LOG}.log 2> ${LOG}.err
### end app
echo "### End at `date +%Y%m%d%H%M%S`" >> ${LOG}.log