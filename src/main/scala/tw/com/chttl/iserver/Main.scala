package tw.com.chttl.iserver

import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SchemaRDD, Row}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
import tw.com.chttl.spark.core.util._
import tw.com.chttl.spark.mllib.util._
import java.sql.{Statement, Connection, DriverManager, ResultSet}
import java.util.Calendar

/**
 * Created by leorick on 2015/9/9.
 */
object Main {
  val appName = "iServer Log ETL"
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

  def saveRecords(records: RDD[(BasicRecord, ArrayBuffer[ComplexRecord])], basicpath:String, complexpath:String ) = {
    val saveCnts = new Array[Long](2)
    // coalesce(64) may cause case class not found exception
    records.map{ case (basic, ary) => basic
    }.map{ case BasicRecord(now:Long, id:Long, cpu_usage:Double
    , mem_phy_usage:Double, mem_cache_usage:Double, mem_load:Double
    , net_out:Double, net_in:Double, net_pkt_send_err:Double, net_pkt_recv_err:Double
    , cdate:Long, ftime:Byte) =>
      Array(now.toString, id.toString, cpu_usage.toString
      , mem_phy_usage.toString, mem_cache_usage.toString, mem_load.toString
      , net_out.toString, net_in.toString, net_pkt_send_err.toString, net_pkt_recv_err.toString
      , cdate.toString, ftime.toString
      ).mkString(_SEPARATOR)
    }.saveAsTextFile(basicpath, classOf[org.apache.hadoop.io.compress.SnappyCodec])
    // coalesce(64) may cause case class not found exception
    records.flatMap{ case (basic, ary) => ary
    }.map{ case ComplexRecord(now:Long, id:Long, cate:Byte, item:String, usage:Double, cdate:Long, ftime:Byte) =>
      Array(now.toString, id.toString, cate.toString, item, usage.toString, cdate.toString, ftime.toString
      ).mkString(_SEPARATOR)
    }.saveAsTextFile(complexpath, classOf[org.apache.hadoop.io.compress.SnappyCodec])
    //
    saveCnts(0) = records.map{ case (basic, ary) => basic}.count()
    saveCnts(1) = records.flatMap{ case (basic, ary) => ary}.count()
    saveCnts
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

  def loadRecords2Table(tblInfo:Array[(String, String, String)], dbInfo:Array[String]) = {
    /*
        var url="jdbc:hive2://10.176.32.44:21050"
        var username = "leoricklin"
        var password = "leoricklin"
        var yarnqueue = "root.PERSONAL.leoricklin"
    */
    val Array( jdbcurl, jdbcdb, jdbcuser, jdbcpwd, yarnqueue ) = dbInfo
    var driverName="org.apache.hive.jdbc.HiveDriver"
    var conn: Connection = null
    var loadCnts = Array(0L, 0L)
    try {
      Class.forName(driverName).newInstance
      conn = DriverManager.getConnection(jdbcurl, jdbcuser, jdbcpwd)
      val stmt: Statement = conn.createStatement()
      var sqls = Array("")
      // init DB env
      sqls = Array(f"set REQUEST_POOL='${yarnqueue}'"
        ,f"use ${jdbcdb}")
      sqls.foreach(sql => stmt.execute(sql) ) // true if the first result is a ResultSet object; false if it is an update count or there are no results
      // load into staging table from saved files
      sqls = tblInfo.map{ case (path, stag, tbl) =>
        f"load data inpath '${path}' OVERWRITE into table ${stag}"
      }
      sqls.foreach{sql => stmt.execute(sql) }
/*
      for (
        tbls <- tblInfo
      ) yield {
        sql = f"load data inpath '${tbls._1}' into table ${tbls._2}"
        stmt.execute(sql) // true if the first result is a ResultSet object; false if it is an update count or there are no results
      }
*/
      // select count from staging table
      sqls = tblInfo.map { case (path, stag, tbl) =>
        f"select count(1) from ${stag}"
      }
      loadCnts = sqls.map{sql =>
        val ret: ResultSet = stmt.executeQuery(sql)
        if (ret.next()) ret.getLong(1) else 0L
      }
/*
      selectCnts = for (
        tbls <- tblPaths
      ) yield {
        sql = f"select count(1) from ${tbls._2}"
        val rets = stmt.executeQuery(sql)
        if (rets.next()) rets.getLong(1) else 0L
      }
*/
      // insert into partitioned table from staging tables
      sqls = Array("set COMPRESSION_CODEC=snappy"
        ,"insert into basic_record ( report_time,agent_id,cpu_usage,mem_phy_usage,mem_cache_usage,mem_load,net_out,net_in,net_pkt_send_err,net_pkt_recv_err) partition (cdate, ftime) select report_time,agent_id,cpu_usage,mem_phy_usage,mem_cache_usage,mem_load,net_out,net_in,net_pkt_send_err,net_pkt_recv_err,cdate,ftime from basic_record_stag"
        ,"insert into complex_record (report_time,agent_id,category,item_name,usage) partition (cdate, ftime) select report_time,agent_id,category,item_name,usage,cdate,ftime from complex_record_stag"
        ,"set COMPRESSION_CODEC=NONE")
      /*
            sql = "set COMPRESSION_CODEC=snappy"
            stmt.execute(sql)
            sql = "insert into basic_record ( report_time,agent_id,cpu_usage,mem_phy_usage,mem_cache_usage,mem_load,net_out,net_in,net_pkt_send_err,net_pkt_recv_err) partition (cdate, ftime) select report_time,agent_id,cpu_usage,mem_phy_usage,mem_cache_usage,mem_load,net_out,net_in,net_pkt_send_err,net_pkt_recv_err,cdate,ftime from basic_record_stag"
            stmt.executeUpdate(sql) // result is 0
            sql = "insert into complex_record (report_time,agent_id,category,item_name,usage) partition (cdate, ftime) select report_time,agent_id,category,item_name,usage,cdate,ftime from complex_record_stag"
            stmt.executeUpdate(sql) // result is 0
            sql = "set COMPRESSION_CODEC=NONE"
            stmt.execute(sql)
      */
      sqls.foreach{sql => stmt.execute(sql) }
      // drop staging table
      /*
            sql = "drop table if exists basic_record_stag"
            stmt.execute(sql)
            sql = "drop table if exists complex_record_stag"
            stmt.execute(sql)
      */
      loadCnts
    } catch {
      case e:Exception => System.err.println(e.printStackTrace())
    } finally {
      conn.close()
    }
    loadCnts
  }

  /*
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

Array(103,492,918, 1,044,461,739)
19 	count at <console>:44 	        2015/09/23 10:47:56 	33 s 	    1/1 	159/159
18 	saveAsTextFile at <console>:43 	2015/09/23 10:47:03 	51 s 	    1/1 	159/159
17 	count at <console>:49 	        2015/09/23 10:45:22 	1.7 min 	1/1 	159/159
16 	saveAsTextFile at <console>:48 	2015/09/23 10:41:45 	3.6 min 	1/1 	159/159
15 	reduce at JsonRDD.scala:57 	    2015/09/23 10:37:48 	3.9 min 	1/1 	159/159

### round 1.5 test, 1 core / 8GB * 64 executors, application_1441962795736_29047 (no restart spark app)
hdfs dfs -mv /hive/tlbd_upload/iserver/log/20150918-*.gz /hive/tlbd_upload/iserver/log/test/
hdfs dfs -mv /hive/tlbd_upload/iserver/log/20150919-*.gz /hive/tlbd_upload/iserver/log/test/
hdfs dfs -ls /hive/tlbd_upload/iserver/log/test/2015*.gz|wc -l
199
hdfs dfs -du -s /hive/tlbd_upload/iserver/log/test
11,097,604,291  33292812873  /hive/tlbd_upload/iserver/log/test

24 	count at <console>:44 	        2015/09/23 11:07:13 	36 s 	0/1 (1 failed) 	189/199 (6 failed)
23 	saveAsTextFile at <console>:43 	2015/09/23 11:04:32 	2.6 min 	1/1 	199/199 (5 failed)
22 	count at <console>:49 	        2015/09/23 11:03:05 	1.4 min 	1/1 	199/199
21 	saveAsTextFile at <console>:48 	2015/09/23 10:59:13 	3.8 min 	1/1 	199/199 (1 failed)
20 	reduce at JsonRDD.scala:57 	    2015/09/23 10:54:10 	5.0 min 	1/1 	199/199

### round 1.6 test, 1 core / 8GB * 64 executors, application_1441962795736_29047 (no restart spark app)
hdfs dfs -ls /hive/tlbd_upload/iserver/log/test/2015*.gz|wc -l
199
hdfs dfs -du -s /hive/tlbd_upload/iserver/log/test
11,097,604,291  33292812873  /hive/tlbd_upload/iserver/log/test

Array(128898598, 1289759999)
33 	count at <console>:91 	        2015/09/23 12:30:27 	2.0 min 	1/1   199/199
32  count at <console>:90 	        2015/09/23 12:27:42 	2.8 min 	1/1   199/199 (13 failed)
31  saveAsTextFile at <console>:88 	2015/09/23 12:24:36 	3.1 min 	1/1   199/199 (7 failed)
30  saveAsTextFile at <console>:82 	2015/09/23 12:18:34 	6.0 min 	1/1   199/199 (2 failed)
29  reduce at JsonRDD.scala:57 	    2015/09/23 12:12:40 	5.9 min 	1/1   199/199
   */
/*
val args = Array("hdfs:///hive/tlbd_upload/iserver/log/small"
,"hdfs:///hive/tlbd_upload/iserver/txt/basic"   , "basic_record_stag"    , "basic_record"
,"hdfs:///hive/tlbd_upload/iserver/txt/complex" , "complex_record_stag"  , "complex_record"
,"root.PERSONAL.leoricklin", "jdbc:hive2://10.176.32.44:21050", "tlbd", "leoricklin", "leoricklin"
)
 */

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    //
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._

    try {
      if (args.length != 12) {
        println(
          "Usage: <app_name> <input_path>                                 \\ \n" +
          " <basic_output_path>   <basic_stag_tblname>   <basic_tblname>  \\ \n" +
          " <complex_output_path> <complex_stag_tblname> <complex_tblname>\\ \n" +
          " <yarn_queue> <jdbc_url> <jdbc_db> <jdbc_user> <jdbc_pwd>"
        )
        System.exit(1)
      }
      val tx = System.currentTimeMillis()
      val Array(inpath
        , basicoutpath,   basic_stag_tbl,   basic_tbl
        , complexoutpath, complex_stag_tbl, complex_tbl
        , yarnqueue, jdbcurl, jdbcdb, jdbcuser, jdbcpwd) = args
      val logDF: SchemaRDD = loadSrc(sc, sqlContext, inpath)
      val records: RDD[(BasicRecord, ArrayBuffer[ComplexRecord])] = parseDF(sqlContext, logDF)
      records.persist(StorageLevel.MEMORY_AND_DISK)
      val saveCnts: Array[Long] = saveRecords(records, f"${basicoutpath}.${tx}", f"${complexoutpath}.${tx}")
      val tblInfo = Array(
        (f"${basicoutpath}.${tx}"  , basic_stag_tbl  , basic_tbl)
       ,(f"${complexoutpath}.${tx}", complex_stag_tbl, complex_tbl))
      val dbInfo: Array[String] = Array( jdbcurl, jdbcdb, jdbcuser, jdbcpwd, yarnqueue )
      val loadCnts: Array[Long] = loadRecords2Table(tblInfo, dbInfo)
      System.out.println()
    } catch {
      case e: org.apache.hadoop.mapred.InvalidInputException => System.err.println(e.getMessage)
    }
  }
/*
load2table
### round 1.6 test, 1 core / 8GB * 64 executors, application_1441962795736_29047 (no restart spark app)
Array(128898598, 1289759999)
14:26 ~ 14:30
loadCnts: Array[Long] = Array(128898598, 1289759999)
 */
}
