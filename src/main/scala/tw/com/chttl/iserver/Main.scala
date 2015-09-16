package tw.com.chttl.iserver

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SchemaRDD, Row}
import scala.collection.mutable.ArrayBuffer
import tw.com.chttl.spark.core.util._
import tw.com.chttl.spark.mllib.util._
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Statement, Connection, DriverManager, ResultSet}

/**
 * Created by leorick on 2015/9/9.
 */
object Main {
  val appName = "iServer Log ETL"
  val sparkConf = new SparkConf().setAppName(appName)
  val sc = new SparkContext(sparkConf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext._
  val _SEPARATOR = "\t"
  sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
  sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
  sqlContext.getConf("spark.sql.hive.convertMetastoreParquet")
  sqlContext.getConf("spark.sql.parquet.binaryAsString")

  case class BasicRecord(now:Long, id:Long, cpu_usage:Double
  , mem_phy_usage:Double, mem_cache_usage:Double, mem_load:Double
  , net_out:Double, net_in:Double, net_pkt_send_err:Double, net_pkt_recv_err:Double)

  case class ComplexRecord(now:Long, id:Long, cate:Byte, item:String, usage:Double)

  def loadSrc(sc:SparkContext, path:String): RDD[String] = {
    sc.textFile(path)
  }

  def tokenize(raw:RDD[String]) = {
    raw.map{ line => StringHelper.tokenize(line,"\t\t",true) }
  }
  /*
  val stats = NAStat.statsWithMissing(tokens.map{ ary => Array(ary.size)})
   */

  def getDF(sqlContext:org.apache.spark.sql.SQLContext, tokens:RDD[Array[String]]) = {
    sqlContext.jsonRDD( tokens.map{ tokens => tokens(1) } )
  }
  /*
1 core/2 GB * 64 workers with 277 files of total 2.3GB = 1.2 mins
  logs.printSchema()
root
|-- AgentData: struct (nullable = true)
|    |-- CPUItem: struct (nullable = true)
|    |    |-- Name: string (nullable = true)
|    |    |-- Usage: string (nullable = true)
|    |-- HD: array (nullable = true)
|    |    |-- element: struct (containsNull = false)
|    |    |    |-- Name: string (nullable = true)
|    |    |    |-- Usage: string (nullable = true)
|    |-- ID: string (nullable = true)
|    |-- LV: array (nullable = true)
|    |    |-- element: struct (containsNull = false)
|    |    |    |-- Name: string (nullable = true)
|    |    |    |-- Usage: string (nullable = true)
|    |-- MemoryItem: array (nullable = true)
|    |    |-- element: struct (containsNull = false)
|    |    |    |-- Name: string (nullable = true)
|    |    |    |-- Usage: string (nullable = true)
|    |-- NetworkItem: array (nullable = true)
|    |    |-- element: struct (containsNull = false)
|    |    |    |-- Name: string (nullable = true)
|    |    |    |-- Usage: string (nullable = true)
|    |-- NowTime: string (nullable = true)
|    |-- PartitionItem: array (nullable = true)
|    |    |-- element: struct (containsNull = false)
|    |    |    |-- Name: string (nullable = true)
|    |    |    |-- Usage: string (nullable = true)
|    |-- Version: string (nullable = true)
|-- id: string (nullable = true)

  logs.count = 2,586,182

  logs.registerTempTable("logs")
  val t41 = sqlContext.sql("select AgentData.NowTime, id" +
    " ,AgentData.CPUItem.Usage as cpu_usage" +
    " ,AgentData.MemoryItem as mem_items" +
    " ,AgentData.NetworkItem as net_items" +
    " from logs" +
    " where AgentData.NowTime = '1441872159969' and id = 31208")
  t42.collect: Array[Row]
   = Array([1441872159969,31208,0.098273
   ,ArrayBuffer([phy_usage,61.927822], [cache_usage,0.043043], [memory_load,40.852562])
   ,ArrayBuffer([outbound,3], [inbound,303], [packet_send_error,0], [packet_recv_error,0])])

  val t43 = sqlContext.sql("select AgentData.NowTime, id" +
    " ,AgentData.HD as hds" +
    " ,AgentData.LV as lvs" +
    " ,AgentData.PartitionItem as pars" +
    " from logs" +
    " where AgentData.NowTime = '1441872159969' and id = 31208")
  t43.collect: Array[Row] = Array([1441872159969,31208
  ,ArrayBuffer([sda,114])
  ,ArrayBuffer()
  ,ArrayBuffer([/,33.390724], [/app,34.528301], [/boot,17.891323], [/var,2.946591])])

   */

  def parseLog(sqlContext:org.apache.spark.sql.SQLContext, logs:SchemaRDD) = {
    logs.registerTempTable("logs")
    val sqlRdd = sqlContext.sql("select AgentData.NowTime, id" +
      " ,AgentData.CPUItem.Usage as cpu_usage" +
      " ,AgentData.MemoryItem  as mem_items" +
      " ,AgentData.NetworkItem as net_items" +
      " ,AgentData.HD as hds" +
      " ,AgentData.LV as lvs" +
      " ,AgentData.PartitionItem as pars" +
      " from logs")
    val records = sqlRdd.map{ case Row(nowtime:String, id:String
    , cpu_usage:String, memitems:ArrayBuffer[Row], netitems:ArrayBuffer[Row]
    , hds:ArrayBuffer[Row], lvs:ArrayBuffer[Row], pars:ArrayBuffer[Row]) =>
      val r_now = nowtime.toLong
      val r_id = id.toLong
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
      //
      val rows = new ArrayBuffer[ComplexRecord]()
      val r_hd:Byte = 1
      val r_lv:Byte = 2
      val r_pr:Byte = 3
      rows ++= hds.map{  case Row(item:String, usage:String) => new ComplexRecord(r_now, r_id, r_hd, item, usage.toDouble) }
      rows ++= lvs.map{  case Row(item:String, usage:String) => new ComplexRecord(r_now, r_id, r_lv, item, usage.toDouble) }
      rows ++= pars.map{ case Row(item:String, usage:String) => new ComplexRecord(r_now, r_id, r_pr, item, usage.toDouble) }
      ( new BasicRecord(r_now, r_id, cpu_usage.toDouble, mems._1, mems._2, mems._3, nets._1, nets._2, nets._3, nets._4)
        ,rows
      )
    }
    records
  }
  /*
  val t11 = sqlContext.sql("select AgentData.NowTime, AgentData.id" +
    " ,AgentData.CPUItem.Usage as cpu_usage" +
    " ,AgentData.MemoryItem as mem_items" +
    " ,AgentData.NetworkItem as net_items" +
    " from logs")

  t11.firstl: Row = [1441735471703,31972,3.391990
  ,ArrayBuffer([phy_usage,0.000000], [cache_usage,0.168925], [memory_load,66.758598])
  ,ArrayBuffer([outbound,26], [inbound,1], [packet_send_error,0], [packet_recv_error,0])]

  val t12 = t11.map{ case Row(nowtime:String, id:String, cpu_usage:String, memitems:ArrayBuffer[Row], netitems:ArrayBuffer[Row]) =>
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
    (nowtime.toLong, id.toLong, cpu_usage.toDouble, mems._1, mems._2, mems._3, nets._1, nets._2, nets._3, nets._4)
  }

  t12.first() : (String, String, Double, Double, Double, Double, Double, Double, Double, Double)
   = (1441735471703,31972,3.39199,0.0,0.168925,66.758598,26.0,1.0,0.0,0.0)

  tokens.map{ case Array(head, json) => json
  }.filter{ json => json.contains("1441735471703") && json.contains("31972")
  }.collect().foreach(println)
{"id":"31972","AgentData":{
"LV":[{"Name":"aplv","Usage":"20.000000"},{"Name":"datalv","Usage":"45.000000"},{"Name":"dumplv01","Usage":"0.000000"},{"Name":"fslv00","Usage":"74.000000"},{"Name":"fslv01","Usage":"0.000000"},{"Name":"hd1","Usage":"0.000000"},{"Name":"hd10opt","Usage":"5.000000"},{"Name":"hd2","Usage":"71.000000"},{"Name":"hd3","Usage":"32.000000"},{"Name":"hd4","Usage":"10.000000"},{"Name":"hd5","Usage":"0.000000"},{"Name":"hd6","Usage":"0.000000"},{"Name":"hd8","Usage":"0.000000"},{"Name":"hd9var","Usage":"70.000000"},{"Name":"lg_dumplv","Usage":"0.000000"},{"Name":"loglv02","Usage":"0.000000"},{"Name":"loglv03","Usage":"0.000000"},{"Name":"paging00","Usage":"0.000000"}]
,"HD":[{"Name":"hdisk0","Usage":"5872"},{"Name":"hdisk1","Usage":"306"},{"Name":"cd0","Usage":"0"},{"Name":"hdisk2","Usage":"943"},{"Name":"hdisk5","Usage":"6"},{"Name":"hdisk4","Usage":"3"},{"Name":"hdisk3","Usage":"931"}]
,"NetworkItem":[{"Name":"outbound","Usage":"26"},{"Name":"inbound","Usage":"1"},{"Name":"packet_send_error","Usage":"0"},{"Name":"packet_recv_error","Usage":"0"}]
,"NowTime":"1441735471703","ID":"31972"
,"PartitionItem":[{"Name":"/","Usage":"10.000000"},{"Name":"/home","Usage":"18.000000"},{"Name":"/oldhome","Usage":"61.000000"},{"Name":"/opt","Usage":"5.000000"},{"Name":"/proc","Usage":"0.000000"},{"Name":"/sw","Usage":"74.000000"},{"Name":"/tmp","Usage":"32.000000"},{"Name":"/u01","Usage":"20.000000"},{"Name":"/u02","Usage":"45.000000"},{"Name":"/usr","Usage":"71.000000"},{"Name":"/var","Usage":"70.000000"}]
,"CPUItem":{"Name":"cpu_1","Usage":"3.391990"}
,"MemoryItem":[{"Name":"phy_usage","Usage":"0.000000"},{"Name":"cache_usage","Usage":"0.168925"},{"Name":"memory_load","Usage":"66.758598"}],"Version":"3.3.1"}}

  val t13 = t11.map{ case Row(nowtime:String, id:String, cpu_usage:String, memitems:ArrayBuffer[Row], netitems:ArrayBuffer[Row]) =>
    Array(id.toDouble)
  }
  NAStat.statsWithMissing(t23): Array[NAStatCounter]
   = Array(stats: + (count: 19070, mean: 24661.342842, stdev: 8188.052727, max: 33936.000000, min: 560.000000) + NaN: + 0)

  val t21 = sqlContext.sql("select AgentData.NowTime, id" +
    " ,AgentData.HD as hds" +
    " ,AgentData.LV as lvs" +
    " ,AgentData.PartitionItem as pars" +
    " from logs")

  t21.first : Row = [1441735471703,31972
  ,ArrayBuffer([hdisk0,5872], [hdisk1,306], [cd0,0], [hdisk2,943], [hdisk5,6], [hdisk4,3], [hdisk3,931])
  ,ArrayBuffer([aplv,20.000000], [datalv,45.000000], [dumplv01,0.000000], [fslv00,74.000000], [fslv01,0.000000], [hd1,0.000000], [hd10opt,5.000000], [hd2,71.000000], [hd3,32.000000], [hd4,10.000000], [hd5,0.000000], [hd6,0.000000], [hd8,0.000000], [hd9var,70.000000], [lg_dumplv,0.000000], [loglv02,0.000000], [loglv03,0.000000], [paging00,0.000000])
  ,ArrayBuffer([/,10.000000], [/home,18.000000], [/oldhome,61.000000], [/opt,5.000000], [/proc,0.000000], [/sw,74.000000], [/tmp,32.000000], [/u01,20.000000], [/u02,45.000000], [/usr,71.000000], [/var,70.000000])]

  val t22 = t21.flatMap{ case Row(nowtime:String, id:String, hds:ArrayBuffer[Row], lvs:ArrayBuffer[Row], pars:ArrayBuffer[Row]) =>
    val rows = new ArrayBuffer[(Long, Long, Byte, String, Double)]()
    val r_now = nowtime.toLong
    val r_id = id.toLong
    val r_hd:Byte = 1
    val r_lv:Byte = 2
    val r_pr:Byte = 3
    rows ++= hds.map{  case Row(item:String, usage:String) => (r_now, r_id, r_hd, item, usage.toDouble) }
    rows ++= lvs.map{  case Row(item:String, usage:String) => (r_now, r_id, r_lv, item, usage.toDouble) }
    rows ++= pars.map{ case Row(item:String, usage:String) => (r_now, r_id, r_pr, item, usage.toDouble) }
    rows
  }

  t22.filter{ case (now:Long, id:Long, cat:Byte, name:String, usage:Double) =>
    (id == 31972L) && (now == 1441735471703L)
  }.collect.foreach(println)
(1441735471703,31972,1,hdisk0,5872.0)
(1441735471703,31972,1,hdisk1,306.0)
(1441735471703,31972,1,cd0,0.0)
(1441735471703,31972,1,hdisk2,943.0)
(1441735471703,31972,1,hdisk5,6.0)
(1441735471703,31972,1,hdisk4,3.0)
(1441735471703,31972,1,hdisk3,931.0)
(1441735471703,31972,2,aplv,20.0)
(1441735471703,31972,2,datalv,45.0)
(1441735471703,31972,2,dumplv01,0.0)
(1441735471703,31972,2,fslv00,74.0)
(1441735471703,31972,2,fslv01,0.0)
(1441735471703,31972,2,hd1,0.0)
(1441735471703,31972,2,hd10opt,5.0)
(1441735471703,31972,2,hd2,71.0)
(1441735471703,31972,2,hd3,32.0)
(1441735471703,31972,2,hd4,10.0)
(1441735471703,31972,2,hd5,0.0)
(1441735471703,31972,2,hd6,0.0)
(1441735471703,31972,2,hd8,0.0)
(1441735471703,31972,2,hd9var,70.0)
(1441735471703,31972,2,lg_dumplv,0.0)
(1441735471703,31972,2,loglv02,0.0)
(1441735471703,31972,2,loglv03,0.0)
(1441735471703,31972,2,paging00,0.0)
(1441735471703,31972,3,/,10.0)
(1441735471703,31972,3,/home,18.0)
(1441735471703,31972,3,/oldhome,61.0)
(1441735471703,31972,3,/opt,5.0)
(1441735471703,31972,3,/proc,0.0)
(1441735471703,31972,3,/sw,74.0)
(1441735471703,31972,3,/tmp,32.0)
(1441735471703,31972,3,/u01,20.0)
(1441735471703,31972,3,/u02,45.0)
(1441735471703,31972,3,/usr,71.0)
(1441735471703,31972,3,/var,70.0)

    t32.map{ case (tup, ary) => tup}.filter{ case (now, id, cpu_usage, mem_phy_usage, mem_cache_usage, mem_load, net_out, net_in, net_pkt_send_err, net_pkt_recv_err) =>
      (now == 1441735471703L && id == 31972L)
    }.collect.foreach(println)
    = (1441735471703,31972,3.39199,0.0,0.168925,66.758598,26.0,1.0,0.0,0.0)

    t32.flatMap{ case (tup, ary) => ary}.filter{ case (now, id, cat, item, usage) =>
      (now == 1441735471703L && id == 31972L)
    }.collect.foreach(println)
   */

  def saveBasicRecords(parsedLogs: RDD[BasicRecord], path:String) = {
    val saved = parsedLogs.map{ case BasicRecord(now:Long, id:Long, cpu_usage:Double
    , mem_phy_usage:Double, mem_cache_usage:Double, mem_load:Double
    , net_out:Double, net_in:Double, net_pkt_send_err:Double, net_pkt_recv_err:Double) =>
      Array(now.toString, id.toString, cpu_usage.toString
      , mem_phy_usage.toString, mem_cache_usage.toString, mem_load.toString
      , net_out.toString, net_in.toString, net_pkt_send_err.toString, net_pkt_recv_err.toString
      ).mkString(_SEPARATOR)
    }
    saved.coalesce(64).saveAsTextFile(path, classOf[org.apache.hadoop.io.compress.SnappyCodec])
    saved.count()
  }
  /*
1 core/2 GB * 64 workers with 277 files of total 2.3GB = 40 secs
hdfs dfs -ls  hdfs://nameservice1/hive/tlbd_upload/iserver/parquet/basic.20150914
Found 66 items
-rw-rw----+  3 leoricklin hive          0 2015-09-14 15:51 hdfs://nameservice1/h
ive/tlbd_upload/iserver/parquet/basic.20150914/_SUCCESS
-rw-rw----+  3 leoricklin hive      56519 2015-09-14 15:51 hdfs://nameservice1/hive/tlbd_upload/iserver/parquet/basic.20150914/_metadata
-rw-rw----+  3 leoricklin hive     760389 2015-09-14 15:51 hdfs://nameservice1/hive/tlbd_upload/iserver/parquet/basic.20150914/part-r-1.parquet
-rw-rw----+  3 leoricklin hive     764578 2015-09-14 15:51 hdfs://nameservice1/hive/tlbd_upload/iserver/parquet/basic.20150914/part-r-10.parquet
   */

  def saveComplexRecords(parsedLogs: RDD[ComplexRecord], path:String) = {
    val saved = parsedLogs.map{ case ComplexRecord(now:Long, id:Long, cate:Byte, item:String, usage:Double) =>
      Array(now.toString, id.toString, cate.toString, item, usage.toString).mkString(_SEPARATOR)
    }
    saved.coalesce(64).saveAsTextFile(path, classOf[org.apache.hadoop.io.compress.SnappyCodec])
    saved.count()
  }
  /*
1 core/2 GB * 64 workers with 277 files of total 2.3GB = 30 secs
hdfs dfs -ls  hdfs://nameservice1/hive/tlbd_upload/iserver/parquet/complex.20150914
Found 66 items
-rw-rw----+  3 leoricklin hive          0 2015-09-14 16:28 hdfs://nameservice1/hive/tlbd_upload/iserver/parquet/complex.20150914/_SUCCESS
-rw-rw----+  3 leoricklin hive      25633 2015-09-14 16:28 hdfs://nameservice1/hive/tlbd_upload/iserver/parquet/complex.20150914/_metadata
-rw-rw----+  3 leoricklin hive    1219905 2015-09-14 16:28 hdfs://nameservice1/hive/tlbd_upload/iserver/parquet/complex.20150914/part-r-1.parquet
-rw-rw----+  3 leoricklin hive    1228008 2015-09-14 16:28 hdfs://nameservice1/hive/tlbd_upload/iserver/parquet/complex.20150914/part-r-10.parquet
   */

  def readBasicRecords(sc:SparkContext, path:String) = {
    val basicRecords = sc.textFile(path).map{line => line.split(_SEPARATOR)}.map{
      case Array(now, id, cpu_usage
      , mem_phy_usage, mem_cache_usage, mem_load
      , net_out, net_in, net_pkt_send_err, net_pkt_recv_err) =>
        BasicRecord(now.toLong, id.toLong, cpu_usage.toDouble
          , mem_phy_usage.toDouble, mem_cache_usage.toDouble, mem_load.toDouble
          , net_out.toDouble, net_in.toDouble, net_pkt_send_err.toDouble, net_pkt_recv_err.toDouble)
    }
    basicRecords
  }
  /*
  val basicRecords = readBasicRecords(sc, f"${basicoutpath}.${tx.toString}")
  basicRecords.printSchema()
  basicRecords.count = 2586182
root
|-- now: long (nullable = false)
|-- id: long (nullable = false)
|-- cpu_usage: double (nullable = false)
|-- mem_phy_usage: double (nullable = false)
|-- mem_cache_usage: double (nullable = false)
|-- mem_load: double (nullable = false)
|-- net_out: double (nullable = false)
|-- net_in: double (nullable = false)
|-- net_pkt_send_err: double (nullable = false)
|-- net_pkt_recv_err: double (nullable = false)

    basicRecords.registerTempTable("basic_record")
    val t41 = sqlContext.sql("select now, id, cpu_usage" +
      " , mem_phy_usage, mem_cache_usage, mem_load" +
      " , net_out, net_in, net_pkt_send_err, net_pkt_recv_err" +
      " from basic_record" +
      " where now = 1441872159969 and id = 31208")
    t41.collect : Array[Row]
     = Array([1441872159969,31208,0.098273,61.927822,0.043043,40.852562,3.0,303.0,0.0,0.0])
   */

  def readComplexRecords(sc:SparkContext, path:String) = {
    val complexRecords = sc.textFile(path).map{line => line.split(_SEPARATOR)}.map{
      case Array(now, id, cate, item, usage) =>
        ComplexRecord(now.toLong, id.toLong, cate.toByte, item:String, usage.toDouble)
    }
    complexRecords
  }
  /*
  val complexRecords = readComplexRecords(sc, f"${complexoutpath}.${tx.toString}")
  complexRecords.printSchema
root
|-- now: long (nullable = false)
|-- id: long (nullable = false)
|-- cate: byte (nullable = false)
|-- item: string (nullable = true)
|-- usage: double (nullable = false)
  complexRecords.count = 26964238
  complexRecords.registerTempTable("complex_record")
  val t43 = sqlContext.sql("select now, id, cate" +
    " , item, usage" +
    " from complex_record" +
    " where now = 1441872159969 and id = 31208")
  t43.collect() : Array[org.apache.spark.sql.Row] = Array(
    [1441872159969,31208,1,sda,114.0]
  , [1441872159969,31208,3,/,33.390724]
  , [1441872159969,31208,3,/app,34.528301]
  , [1441872159969,31208,3,/boot,17.891323]
  , [1441872159969,31208,3,/var,2.946591])
   */

  def load2Hive(basicoutpath:String, complexoutpath:String, partionid:Long) = {
    var url="jdbc:hive2://10.176.32.79:10000/tlbd?mapred.job.queue.name=root.PERSONAL.leoricklin"
    var username = "leoricklin"
    var password = "leoricklin"
    var driverName="org.apache.hive.jdbc.HiveDriver"
    Class.forName(driverName).newInstance
    val conn: Connection = DriverManager.getConnection(url, username, password)
    val stmt: Statement = conn.createStatement()
    var query = f"load data inpath '${basicoutpath}' into table basic_record partition (cdate=${partionid})"
    var result = stmt.execute(query) // true if the first result is a ResultSet object; false if it is an update count or there are no results
    query = f"select count(1) from basic_record where cdate=${partionid}"
    var resultset = stmt.executeQuery(query)
    val basicRecCnt = if (resultset.next()) resultset.getLong(1) else 0L // 2586182
    //
    query = f"load data inpath '${complexoutpath}' into table complex_record partition (cdate=${partionid})"
    result = stmt.execute(query)
    query = f"select count(1) from complex_record where cdate=${partionid}"
    resultset = stmt.executeQuery(query)
    val complexRecCnt = if (resultset.next()) resultset.getLong(1) else 0L // 26964238
    conn.close()
    Array(basicRecCnt, complexRecCnt)
  }
  /*
    val basicRecords: JdbcRDD[BasicRecord] = new JdbcRDD( sc
    , () => DriverManager.getConnection(url,username,password)
    , query
    , 1441843200000L, 1441929599000L, 64
    , r => BasicRecord(r.getLong(1),r.getLong(2), r.getDouble(3)
      , r.getDouble(4), r.getDouble(5), r.getDouble(6)
      , r.getDouble(7), r.getDouble(8), r.getDouble(9), r.getDouble(10))
    )

    res4: Any = time=1441814401969, id=22640

    query = f"select * from basic_record where cdate=${parttionid} limit 10"
    var resultset = stmt.executeQuery(query)
    while (resultset.next()) {
      println(f"time=${resultset.getLong(1)}, id=${resultset.getLong(2)}")
    }
time=1441814401969, id=22640
time=1441814401969, id=22824
time=1441814401969, id=25172
time=1441814401969, id=31844
time=1441814401969, id=32244
time=1441814401969, id=32688
time=1441814401969, id=33264
time=1441814403000, id=11416
time=1441814403000, id=24104
time=1441814403000, id=32084
   */

  def main(args: Array[String]) {
    /*
val args = Array("/home/leoricklin/dataset/iserver")

val args = Array("hdfs:///hive/tlbd_upload/iserver/log"
,"hdfs:///hive/tlbd_upload/iserver/txt/basic"
,"hdfs:///hive/tlbd_upload/iserver/txt/complex")

$ hdfs dfs -du -s /hive/tlbd_upload/iserver/log
2,310,400,905  6931202715  /hive/tlbd_upload/iserver/log
$ hdfs dfs -ls /hive/tlbd_upload/iserver/log|wc -l
278
     */
    try {
      if (args.length != 3) {
        println("Usage: <app_name> <input_path> <basic_output_path> <complex_output_path>")
        System.exit(1)
      }
      val tx = System.currentTimeMillis()
      val Array(inpath, basicoutpath, complexoutpath) = args
      val raw: RDD[String] = loadSrc(sc, inpath)
      val tokens: RDD[Array[String]] = tokenize(raw)
      val logs: SchemaRDD = getDF(sqlContext, tokens)
      val records: RDD[(BasicRecord, ArrayBuffer[ComplexRecord])] = parseLog(sqlContext, logs)
      val basicCnt: Long = saveBasicRecords(records.map{ case (basic, ary) => basic}, f"${basicoutpath}.${tx.toString}")
      val complexCnt: Long = saveComplexRecords(records.flatMap{ case (basic, ary) => ary}, f"${complexoutpath}.${tx.toString}")
      // 2586182, 26964238
      val cnts: Array[Long] = load2Hive(f"${basicoutpath}.${tx.toString}", f"${complexoutpath}.${tx.toString}", 20150916L)
      // Array(2586182, 26964238)
    } catch {
      case e: org.apache.hadoop.mapred.InvalidInputException => System.err.println(e.getMessage)
    }
  }
}
