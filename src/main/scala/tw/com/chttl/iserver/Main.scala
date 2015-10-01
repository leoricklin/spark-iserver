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
  val appName = "iserver_log_etl"
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

  def saveRecords(numexecs:Int, records: RDD[(BasicRecord, ArrayBuffer[ComplexRecord])], basicpath:String, complexpath:String ) = {
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
    }.coalesce(numexecs, false).saveAsTextFile(basicpath, classOf[org.apache.hadoop.io.compress.SnappyCodec])
    // coalesce(64) may cause case class not found exception
    records.flatMap{ case (basic, ary) => ary
    }.map{ case ComplexRecord(now:Long, id:Long, cate:Byte, item:String, usage:Double, cdate:Long, ftime:Byte) =>
      Array(now.toString, id.toString, cate.toString, item, usage.toString, cdate.toString, ftime.toString
      ).mkString(_SEPARATOR)
    }.coalesce(numexecs, false).saveAsTextFile(complexpath, classOf[org.apache.hadoop.io.compress.SnappyCodec])
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

  def loadRecords2Table(tx:Long, tblInfo:Array[(String, String, String)], dbInfo:Array[String]) = {
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
        ,f"use ${jdbcdb}"
        ,"""create table if not exists complex_record_stag (report_time BIGINT,agent_id BIGINT,category TINYINT,item_name STRING,usage DOUBLE,cdate BIGINT,ftime TINYINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE"""
        ,"""create table if not exists basic_record_stag (report_time BIGINT,agent_id BIGINT,cpu_usage DOUBLE,mem_phy_usage DOUBLE,mem_cache_usage DOUBLE,mem_load DOUBLE,net_out DOUBLE,net_in DOUBLE,net_pkt_send_err DOUBLE,net_pkt_recv_err DOUBLE,cdate BIGINT,ftime TINYINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE"""
      )
      // true if the first result is a ResultSet object; false if it is an update count or there are no results
      sqls.foreach(sql => stmt.execute(sql) )
      // load into staging table from saved files
      sqls = tblInfo.map{ case (path, stag, tbl) =>
        f"load data inpath '${path}' OVERWRITE into table ${stag}"
      }
      sqls.foreach{sql => stmt.execute(sql) }
      // select count from staging table
      sqls = tblInfo.map { case (path, stag, tbl) =>
        f"select count(1) from ${stag}"
      }
      loadCnts = sqls.map{sql =>
        val ret: ResultSet = stmt.executeQuery(sql)
        if (ret.next()) ret.getLong(1) else 0L
      }
      // insert into partitioned table from staging tables
      sqls = Array("set COMPRESSION_CODEC=snappy"
        ,"insert into basic_record ( report_time,agent_id,cpu_usage,mem_phy_usage,mem_cache_usage,mem_load,net_out,net_in,net_pkt_send_err,net_pkt_recv_err) partition (cdate, ftime) select report_time,agent_id,cpu_usage,mem_phy_usage,mem_cache_usage,mem_load,net_out,net_in,net_pkt_send_err,net_pkt_recv_err,cdate,ftime from basic_record_stag"
        ,"insert into complex_record (report_time,agent_id,category,item_name,usage) partition (cdate, ftime) select report_time,agent_id,category,item_name,usage,cdate,ftime from complex_record_stag"
        ,"set COMPRESSION_CODEC=NONE")
      sqls.foreach{sql => stmt.execute(sql) }
      // drop staging table
      sqls = Array("drop table if exists basic_record_stag"
        ,"drop table if exists complex_record_stag")
      sqls.foreach{sql => stmt.execute(sql) }
      loadCnts
    } catch {
      case e:Exception => System.err.println(e.printStackTrace())
    } finally {
      conn.close()
    }
    loadCnts
  }

/*
val args = Array("80", "hdfs:///hive/tlbd_upload/iserver/log/small"
,"hdfs:///hive/tlbd_upload/iserver/txt/basic"   , "basic_record_stag"    , "basic_record"
,"hdfs:///hive/tlbd_upload/iserver/txt/complex" , "complex_record_stag"  , "complex_record"
,"root.PERSONAL.leoricklin", "jdbc:hive2://10.176.32.44:21050", "tlbd", "leoricklin", "leoricklin"
)
 */

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val hdpconf = sc.hadoopConfiguration
    // to mitigate HDFS exceptions
    hdpconf.setInt("dfs.replication",2)
    //
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._
    try {
      if (args.length != 13) {
        println(
          f"[${appName}] Usage: <app_name> <num_executors> <input_path>   \\ \n" +
          " <basic_output_path>   <basic_stag_tblname>   <basic_tblname>  \\ \n" +
          " <complex_output_path> <complex_stag_tblname> <complex_tblname>\\ \n" +
          " <yarn_queue> <jdbc_url> <jdbc_db> <jdbc_user> <jdbc_pwd>"
        )
        System.exit(1)
      }
      val tx: Long = System.currentTimeMillis()
      val Array(numexecs, inpath
        , basicoutpath,   basic_stag_tbl,   basic_tbl
        , complexoutpath, complex_stag_tbl, complex_tbl
        , yarnqueue, jdbcurl, jdbcdb, jdbcuser, jdbcpwd) = args
      val logDF: SchemaRDD = loadSrc(sc, sqlContext, inpath)
      val records: RDD[(BasicRecord, ArrayBuffer[ComplexRecord])] = parseDF(sqlContext, logDF)
      // StorageLevel.MEMORY_AND_DISK_2 will cause java.io.IOException: Connection reset by peer (at io.netty.buffer.AbstractByteBuf.writeBytes)
      records.persist(StorageLevel.MEMORY_AND_DISK)
      val saveCnts: Array[Long] = saveRecords(numexecs.toInt, records, f"${basicoutpath}.${tx}", f"${complexoutpath}.${tx}")
      val tblInfo = Array(
        (f"${basicoutpath}.${tx}"  , basic_stag_tbl  , basic_tbl)
       ,(f"${complexoutpath}.${tx}", complex_stag_tbl, complex_tbl))
      val dbInfo: Array[String] = Array( jdbcurl, jdbcdb, jdbcuser, jdbcpwd, yarnqueue )
      val loadCnts: Array[Long] = loadRecords2Table(tx, tblInfo, dbInfo)
      System.out.println(f"[${appName}] ARGS: ${args.mkString("{",",","}")}")
      System.out.println(f"[${appName}] TX: ${tx}")
      System.out.println(f"[${appName}] saved count: ${saveCnts.mkString("{",",","}")}")
      System.out.println(f"[${appName}] loaded count: ${loadCnts.mkString("{",",","}")}")
    } catch {
      case e: Exception => System.err.println(e.getMessage)
    }
  }
}
