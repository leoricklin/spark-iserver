package tw.com.chttl.iserver

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
import tw.com.chttl.spark.core.util._
import tw.com.chttl.spark.mllib.util._
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Statement, Connection, DriverManager, ResultSet}
import java.util.Calendar

/**
 * Created by leorick on 2015/9/9.
 */
object Main {
  val appName = "iServer Log ETL"
  val sparkConf = new SparkConf().setAppName(appName)
  val sc = new SparkContext(sparkConf)
  sc.getConf.set("spark.driver.maxResultSize", "2g")
  sc.getConf.getOption("spark.driver.maxResultSize").getOrElse("")
  //
  import org.apache.spark.SparkContext._
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext._
  val _SEPARATOR: String = "\t"
  val _CAT_HD: Byte = 1
  val _CAT_LV: Byte = 2
  val _CAT_PR: Byte = 3

  case class BasicRecord(now:Long, id:Long, cpu_usage:Double
  , mem_phy_usage:Double, mem_cache_usage:Double, mem_load:Double
  , net_out:Double, net_in:Double, net_pkt_send_err:Double, net_pkt_recv_err:Double, cdate:Long, ftime:Byte)

  case class ComplexRecord(now:Long, id:Long, cate:Byte, item:String, usage:Double, cdate:Long, ftime:Byte)

  def loadSrc(sc:SparkContext, sqlContext:org.apache.spark.sql.SQLContext, path:String) = {
    val raw = sc.textFile(path)
    val tokens = raw.map{ line => StringHelper.tokenize(line,"\t\t",true) }
    sqlContext.jsonRDD( tokens.map{ tokens => tokens(1) } )
  }

  def parseDF(sqlContext:org.apache.spark.sql.SQLContext, logs:SchemaRDD) = {
    logs.registerTempTable("logs")
    val sqlRdd = sqlContext.sql("select AgentData.NowTime, id" +
      " ,AgentData.CPUItem.Usage as cpu_usage" +
      " ,AgentData.MemoryItem  as mem_items" +
      " ,AgentData.NetworkItem as net_items" +
      " ,AgentData.HD as hds" +
      " ,AgentData.LV as lvs" +
      " ,AgentData.PartitionItem as pars" +
      " from logs")
    val records = sqlRdd.mapPartitions{ ite =>
      val cald: Calendar = new java.util.GregorianCalendar()
      ite.map{ case Row(nowtime:String, id:String
      , cpu_usage:String, memitems:ArrayBuffer[Row], netitems:ArrayBuffer[Row]
      , hds:ArrayBuffer[Row], lvs:ArrayBuffer[Row], pars:ArrayBuffer[Row]) =>
        // process basic record
        val r_now = nowtime.toLong
        val r_id = id.toLong
        // process partition columns
        cald.setTimeInMillis(r_now)
        val cdate = f"${cald.get(Calendar.YEAR)}%04d${cald.get(Calendar.MONTH)+1}%02d${cald.get(Calendar.DAY_OF_MONTH)}%02d".toLong
        val ftime = f"${cald.get(Calendar.HOUR_OF_DAY)}%02d".toByte
        val mems: (Double, Double, Double) = memitems.map{
          case Row("phy_usage",   usage:String) => (usage.toDouble, 0D, 0D)
          case Row("cache_usage", usage:String) => (0D, usage.toDouble, 0D)
          case Row("memory_load", usage:String) => (0D, 0D, usage.toDouble)
          case _ => (0D, 0D, 0D)
        }.reduce{ (tup1, tup2) =>
          (tup1._1+tup2._1 , tup1._2+tup2._2 , tup1._3+tup2._3)
        }
        val nets = netitems.map{
          case Row("outbound",          usage:String) => (usage.toDouble, 0D, 0D, 0D)
          case Row("inbound",           usage:String) => (0D, usage.toDouble, 0D, 0D)
          case Row("packet_send_error", usage:String) => (0D, 0D, usage.toDouble, 0D)
          case Row("packet_recv_error", usage:String) => (0D, 0D, 0D, usage.toDouble)
          case _ => (0D, 0D, 0D, 0D)
        }.reduce{ (tup1, tup2) =>
          (tup1._1+tup2._1 , tup1._2+tup2._2 , tup1._3+tup2._3 , tup1._4+tup2._4)
        }
        // process complex record
        val rows = new ArrayBuffer[ComplexRecord]()
        rows ++= hds.map{  case Row(item:String, usage:String) => new ComplexRecord(r_now, r_id, _CAT_HD, item, usage.toDouble, cdate, ftime) }
        rows ++= lvs.map{  case Row(item:String, usage:String) => new ComplexRecord(r_now, r_id, _CAT_LV, item, usage.toDouble, cdate, ftime) }
        rows ++= pars.map{ case Row(item:String, usage:String) => new ComplexRecord(r_now, r_id, _CAT_PR, item, usage.toDouble, cdate, ftime) }
        // process partition key
        // emit
        (
         new BasicRecord(r_now, r_id, cpu_usage.toDouble, mems._1, mems._2, mems._3, nets._1, nets._2, nets._3, nets._4, cdate, ftime)
         ,rows
        )
      }
    }
    records
  }

  def saveBasicRecords(parsedLogs: RDD[BasicRecord], path:String) = {
    val saved = parsedLogs.map{ case BasicRecord(now:Long, id:Long, cpu_usage:Double
    , mem_phy_usage:Double, mem_cache_usage:Double, mem_load:Double
    , net_out:Double, net_in:Double, net_pkt_send_err:Double, net_pkt_recv_err:Double, cdate:Long, ftime:Byte) =>
      Array(now.toString, id.toString, cpu_usage.toString
      , mem_phy_usage.toString, mem_cache_usage.toString, mem_load.toString
      , net_out.toString, net_in.toString, net_pkt_send_err.toString, net_pkt_recv_err.toString
      , cdate.toString, ftime.toString
      ).mkString(_SEPARATOR)
    }
    saved.saveAsTextFile(path, classOf[org.apache.hadoop.io.compress.SnappyCodec])
    saved.count()
  }

  def saveComplexRecords(parsedLogs: RDD[ComplexRecord], path:String) = {
    val saved = parsedLogs.map{ case ComplexRecord(now:Long, id:Long, cate:Byte, item:String, usage:Double, cdate:Long, ftime:Byte) =>
      Array(now.toString, id.toString, cate.toString, item, usage.toString, cdate.toString, ftime.toString
      ).mkString(_SEPARATOR)
    }
    saved.saveAsTextFile(path, classOf[org.apache.hadoop.io.compress.SnappyCodec])
    saved.count()
  }

  def readBasicRecords(sc:SparkContext, path:String) = {
    val basicRecords = sc.textFile(path).map{line => line.split(_SEPARATOR)}.map{
      case Array(now, id, cpu_usage
      , mem_phy_usage, mem_cache_usage, mem_load
      , net_out, net_in, net_pkt_send_err, net_pkt_recv_err, cdate, ftime) =>
        BasicRecord(now.toLong, id.toLong, cpu_usage.toDouble
        ,mem_phy_usage.toDouble, mem_cache_usage.toDouble, mem_load.toDouble
        ,net_out.toDouble, net_in.toDouble, net_pkt_send_err.toDouble, net_pkt_recv_err.toDouble
        ,cdate.toLong, ftime.toByte
        )
    }
    basicRecords
  }

  def readComplexRecords(sc:SparkContext, path:String) = {
    val complexRecords = sc.textFile(path).map{line => line.split(_SEPARATOR)}.map{
      case Array(now, id, cate, item, usage, cdate, ftime) =>
        ComplexRecord(now.toLong, id.toLong, cate.toByte, item:String, usage.toDouble, cdate.toLong, ftime.toByte)
    }
    complexRecords
  }

  def loadRecords2Table(tblPaths:Array[(String, String, String)]) = {
    var url="jdbc:hive2://10.176.32.44:21050"
    // var url="jdbc:hive2://10.176.32.44:21050"
    var username = "leoricklin"
    var password = "leoricklin"
    var driverName="org.apache.hive.jdbc.HiveDriver"
    var conn: Connection = null
    var selectCnts = Array(0L, 0L)
    var sql = ""
    try {
      Class.forName(driverName).newInstance
      conn = DriverManager.getConnection(url, username, password)
      val stmt: Statement = conn.createStatement()
      val initSQLs = Array(
       "set REQUEST_POOL='root.PERSONAL.leoricklin'"
      ,"use tlbd"
      ,"""create table if not exists basic_record_stag (report_time BIGINT,agent_id BIGINT,cpu_usage DOUBLE,mem_phy_usage DOUBLE,mem_cache_usage DOUBLE,mem_load DOUBLE,net_out DOUBLE,net_in DOUBLE,net_pkt_send_err DOUBLE,net_pkt_recv_err DOUBLE,cdate BIGINT,ftime TINYINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE"""
      ,"""create table if not exists complex_record_stag ( report_time BIGINT,agent_id BIGINT,category TINYINT,item_name STRING,usage DOUBLE,cdate BIGINT,ftime TINYINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE"""
      )
      initSQLs.foreach(sql => stmt.execute(sql) )
      // load into staging table
      for (
        tbls <- tblPaths
      ) yield {
        sql = f"load data inpath '${tbls._1}' into table ${tbls._2}"
        stmt.execute(sql) // true if the first result is a ResultSet object; false if it is an update count or there are no results
      }
      // select count from staging table
      selectCnts = for (
        tbls <- tblPaths
      ) yield {
        sql = f"select count(1) from ${tbls._2}"
        val rets = stmt.executeQuery(sql)
        if (rets.next()) rets.getLong(1) else 0L
      }
      // insert into partitioned table
      sql = "set COMPRESSION_CODEC=snappy"
      stmt.execute(sql)
      sql = "insert into basic_record ( report_time,agent_id,cpu_usage,mem_phy_usage,mem_cache_usage,mem_load,net_out,net_in,net_pkt_send_err,net_pkt_recv_err) partition (cdate, ftime) select report_time,agent_id,cpu_usage,mem_phy_usage,mem_cache_usage,mem_load,net_out,net_in,net_pkt_send_err,net_pkt_recv_err,cdate,ftime from basic_record_stag"
      stmt.executeUpdate(sql) // result is 0
      sql = "insert into complex_record (report_time,agent_id,category,item_name,usage) partition (cdate, ftime) select report_time,agent_id,category,item_name,usage,cdate,ftime from complex_record_stag"
      stmt.executeUpdate(sql) // result is 0
      sql = "set COMPRESSION_CODEC=NONE"
      stmt.execute(sql)
      // drop staging table
      sql = "drop table if exists basic_record_stag"
      stmt.execute(sql)
      sql = "drop table if exists complex_record_stag"
      stmt.execute(sql)
      selectCnts
    } catch {
      case e:Exception => System.err.println(e.printStackTrace())
    } finally {
      conn.close()
    }
    selectCnts
  }

  /*
val args = Array("/home/leoricklin/dataset/iserver")

val args = Array("hdfs:///hive/tlbd_upload/iserver/log"
,"hdfs:///hive/tlbd_upload/iserver/txt/basic"
,"hdfs:///hive/tlbd_upload/iserver/txt/complex")

val args = Array("hdfs:///hive/tlbd_upload/iserver/log/test"
,"hdfs:///hive/tlbd_upload/iserver/txt/basic"
,"hdfs:///hive/tlbd_upload/iserver/txt/complex")

### round 1.1 test, 1 core / 8GB * 64 executors, application_1441962795736_29047
### config more MEM, ref http://qnalist.com/questions/5431253/spark-on-yarn-executorlostfailure-for-long-running-computations-in-map
hdfs dfs -mv /hive/tlbd_upload/iserver/log/20150910-*.gz /hive/tlbd_upload/iserver/log/test/
hdfs dfs -ls /hive/tlbd_upload/iserver/log/test/2015*.gz|wc -l
19
hdfs dfs -du -s /hive/tlbd_upload/iserver/log/test
997,104,971  2991314913  /hive/tlbd_upload/iserver/log/test

4 	count at <console>:44 	        2015/09/23 10:01:56 	6 s 	    1/1 	19/19
3 	saveAsTextFile at <console>:43 	2015/09/23 10:01:42 	13 s 	    1/1 	19/19
2 	count at <console>:49 	        2015/09/23 10:01:36 	6 s 	    1/1 	19/19
1 	saveAsTextFile at <console>:48 	2015/09/23 09:59:59 	1.6 min 	1/1 	19/19
0 	reduce at JsonRDD.scala:57 	    2015/09/23 09:58:18 	1.6 min 	1/1 	19/19

### round 1.2 test, 1 core / 8GB * 64 executors, application_1441962795736_29047 (no restart spark app)
hdfs dfs -mv /hive/tlbd_upload/iserver/log/20150911-*.gz /hive/tlbd_upload/iserver/log/test/
hdfs dfs -ls /hive/tlbd_upload/iserver/log/test/2015*.gz|wc -l
39
hdfs dfs -du -s /hive/tlbd_upload/iserver/log/test
2,095,295,851  6285887553  /hive/tlbd_upload/iserver/log/test

Array(24,230,098, 259,536,539)
9 	count at <console>:44 	        2015/09/23 10:12:39 	21 s 	    1/1 	39/39
8 	saveAsTextFile at <console>:43 	2015/09/23 10:12:18 	21 s 	    1/1 	39/39
7 	count at <console>:49 	        2015/09/23 10:12:08 	10 s 	    1/1 	39/39
6 	saveAsTextFile at <console>:48 	2015/09/23 10:10:35 	1.5 min   1/1 	39/39
5 	reduce at JsonRDD.scala:57 	    2015/09/23 10:08:50 	1.7 min   1/1 	39/39

### round 1.3 test, 1 core / 8GB * 64 executors, application_1441962795736_29047 (no restart spark app)
hdfs dfs -mv /hive/tlbd_upload/iserver/log/20150912-*.gz /hive/tlbd_upload/iserver/log/test/
hdfs dfs -mv /hive/tlbd_upload/iserver/log/20150913-*.gz /hive/tlbd_upload/iserver/log/test/
hdfs dfs -ls /hive/tlbd_upload/iserver/log/test/2015*.gz|wc -l
79
hdfs dfs -du -s /hive/tlbd_upload/iserver/log/test
4,406,146,551  13218439653  /hive/tlbd_upload/iserver/log/test

Array(50,927,738, 530,292,319)
14 	count at <console>:44 	        2015/09/23 10:30:08 	25 s 	    1/1 	79/79
13 	saveAsTextFile at <console>:43 	2015/09/23 10:29:32 	35 s 	    1/1 	79/79
12 	count at <console>:49 	        2015/09/23 10:29:12 	20 s 	    1/1 	79/79 (2 failed)
11 	saveAsTextFile at <console>:48 	2015/09/23 10:27:24 	1.8 min 	1/1 	79/79
10 	reduce at JsonRDD.scala:57 	    2015/09/23 10:25:13 	2.2 min 	1/1 	79/79

### round 1.4 test, 1 core / 8GB * 64 executors, application_1441962795736_29047 (no restart spark app)
hdfs dfs -mv /hive/tlbd_upload/iserver/log/20150914-*.gz /hive/tlbd_upload/iserver/log/test/
hdfs dfs -mv /hive/tlbd_upload/iserver/log/20150915-*.gz /hive/tlbd_upload/iserver/log/test/
hdfs dfs -mv /hive/tlbd_upload/iserver/log/20150916-*.gz /hive/tlbd_upload/iserver/log/test/
hdfs dfs -mv /hive/tlbd_upload/iserver/log/20150917-*.gz /hive/tlbd_upload/iserver/log/test/
hdfs dfs -ls /hive/tlbd_upload/iserver/log/test/2015*.gz|wc -l
159
hdfs dfs -du -s /hive/tlbd_upload/iserver/log/test
8,930,302,951  26790908853  /hive/tlbd_upload/iserver/log/test

   */
  def main(args: Array[String]) {
    try {
      if (args.length != 3) {
        println("Usage: <app_name> <input_path> <basic_output_path> <complex_output_path>")
        System.exit(1)
      }
      val tx = System.currentTimeMillis()
      val Array(inpath, basicoutpath, complexoutpath) = args
      val logDF: SchemaRDD = loadSrc(sc, sqlContext, inpath)
      val records: RDD[(BasicRecord, ArrayBuffer[ComplexRecord])] = parseDF(sqlContext, logDF)
      records.persist(StorageLevel.MEMORY_AND_DISK)
      val parseCnts = new Array[Long](2)
      parseCnts(0) = saveBasicRecords(records.map{ case (basic, ary) => basic}, f"${basicoutpath}.${tx}")
      parseCnts(1) = saveComplexRecords(records.flatMap{ case (basic, ary) => ary}, f"${complexoutpath}.${tx}")
      val tblPaths = Array(
        (f"${basicoutpath}.${tx}"  , "basic_record_stag"  , "basic_record")
       ,(f"${complexoutpath}.${tx}", "complex_record_stag", "complex_record"))
      val loadCnts: Array[Long] = loadRecords2Table(tblPaths)
    } catch {
      case e: org.apache.hadoop.mapred.InvalidInputException => System.err.println(e.getMessage)
    }
  }
/*
resources : 1 core / 4g * 64 workers
input     : 484 files with total size of 4,039,541,352
output    :
partKeys    : Array[Long] = Array(20150910, 20150911, 20150912, 20150913, 20150914, 20150915, 20150916, 20150917)
basicCnts   : Array[Long] = Array(615328  , 635940  , 660306  , 674579  , 665556  , 657641  , 652725  , 18)
complexCnts : Array[Long] = Array(7145941 , 6280175 , 6529955 , 7007741 , 6655225 , 6441167 , 6327969 , 138)
duration  : 2015/09/18 14:17:53 ~ 2015/09/18 14:20:44 = 2 min 51 sec

basicCnts: Long = 4562093
complexCnts: Long = 46388311
loadCnts: Array[Long] = Array(4562093, 46388311)
 */
/*

 */
}
