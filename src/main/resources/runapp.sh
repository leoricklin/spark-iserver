### create staging dir and move src files
NOW=`date +%Y%m%d%H%M%S`
LOG=/home/leoricklin/log/spark-${NOW}
SRC=hdfs://nameservice1/hive/tlbd_upload/iserver/log/src
STAG=hdfs://nameservice1/hive/tlbd_upload/iserver/log/${NOW}
hdfs dfs -mkdir ${STAG}
hdfs dfs -mv "${SRC}/*.log.gz" ${STAG}
###
export MAINCLASS=tw.com.chttl.iserver.Main
export JAR=/home/leoricklin/jar/spark-iserver.jar
export QUEUE=root.PERSONAL.leoricklin
export HADOOP_CONF_DIR=/etc/hive/conf
export JARS=file:///home/leoricklin/jar/spark-util.jar
export MASTER=yarn-cluster
export EXE_MEM=8g
export EXE_CORE=2
export EXE_NUM=80
export DRV_MEM=8g
export DRV_CLASS_PATH="'/home/cloudera/parcels/CDH/lib/hive/lib/*'"
export DRV_JAVA_OPT="'-Dspark.executor.extraClassPath=/home/cloudera/parcels/CDH/lib/hive/lib/*'"
export CONFS="--conf spark.driver.maxResultSize=2g"
export CONFS="${CONFS}"
export ARGS="${EXE_NUM} ${STAG}"
export ARGS="${ARGS} hdfs://nameservice1/hive/tlbd_upload/iserver/txt/basic basic_record_stag basic_record"
export ARGS="${ARGS} hdfs://nameservice1/hive/tlbd_upload/iserver/txt/complex complex_record_stag complex_record"
export ARGS="${ARGS} root.PERSONAL.leoricklin jdbc:hive2://10.176.32.44:21050 tlbd leoricklin leoricklin"
###
spark-submit --class ${MAINCLASS} --master ${MASTER} --queue ${QUEUE} --driver-memory ${DRV_MEM} --executor-memory ${EXE_MEM} --executor-cores ${EXE_CORE} --num-executors ${EXE_NUM} --jars ${JARS} --driver-class-path '/home/cloudera/parcels/CDH/lib/hive/lib/*' --driver-java-options '-Dspark.executor.extraClassPath=/home/cloudera/parcels/CDH/lib/hive/lib/*' ${CONFS} ${JAR} ${ARGS} >> ${LOG}.log 2> ${LOG}.err
###
echo "### End at `date +%Y%m%d%H%M%S`" >> ${LOG}.log