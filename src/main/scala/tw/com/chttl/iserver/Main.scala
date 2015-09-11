package tw.com.chttl.iserver

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row
import tw.com.chttl.spark.core.util._
import tw.com.chttl.spark.mllib.util._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by leorick on 2015/9/9.
 */
object Main {
  val appName = "iServer Log ETL"
  val sparkConf = new SparkConf().setAppName(appName)
  val sc = new SparkContext(sparkConf)

  def loadSrc(sc:SparkContext, path:String): RDD[String] = {
    sc.textFile(path)
  }

  def main(args: Array[String]) {
    // val args = Array("/home/leoricklin/dataset/iserver")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._
    try {
      val Array(inpath) = args
      val raw: RDD[String] = loadSrc(sc, inpath)
      val tokens: RDD[Array[String]] = raw.map{ line => StringHelper.tokenize(line,"\t\t",true) }
      /*
      val stats = NAStat.statsWithMissing(tokens.map{ ary => Array(ary.size)})
      stats: Array[NAStatCounter] = Array(
       stats: + (count: 19070, mean: 2.000000, stdev: 0.000000, max: 2.000000, min: 2.000000) + NaN: + 0)
       */
      val logs = sqlContext.jsonRDD( tokens.map{ tokens => tokens(1) } )
      /*
      logs.printSchema()
root
 |-- AgentData: struct (nullable = true)
 |    |-- CPUItem: struct (nullable = true)
 |    |    |-- Name: string (nullable = true)
 |    |    |-- Usage: string (nullable = true)
 |    |-- HD: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- Name: string (nullable = true)
 |    |    |    |-- Usage: string (nullable = true)
 |    |-- ID: string (nullable = true)
 |    |-- LV: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- Name: string (nullable = true)
 |    |    |    |-- Usage: string (nullable = true)
 |    |-- MemoryItem: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- Name: string (nullable = true)
 |    |    |    |-- Usage: string (nullable = true)
 |    |-- NetworkItem: array (nullable = true)
  |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- Name: string (nullable = true)
 |    |    |    |-- Usage: string (nullable = true)
 |    |-- NowTime: string (nullable = true)
 |    |-- PartitionItem: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- Name: string (nullable = true)
 |    |    |    |-- Usage: string (nullable = true)
 |    |-- Version: string (nullable = true)
 |-- id: string (nullable = true)
       */
      logs.registerTempTable("logs")
      val t11 = sqlContext.sql("select AgentData.NowTime, id" +
        " ,AgentData.CPUItem.Usage as cpu_usage" +
        " ,AgentData.MemoryItem as mem_items" +
        " ,AgentData.NetworkItem as net_items" +
        " from logs")
      /*
      t11.firstl: Row = [1441735471703,31972,3.391990
      ,ArrayBuffer([phy_usage,0.000000], [cache_usage,0.168925], [memory_load,66.758598])
      ,ArrayBuffer([outbound,26], [inbound,1], [packet_send_error,0], [packet_recv_error,0])]
       */
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
      /*
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
       */
      val t21 = sqlContext.sql("select AgentData.NowTime, id" +
        " ,AgentData.HD as hds" +
        " ,AgentData.LV as lvs" +
        " ,AgentData.PartitionItem as pars" +
        " from logs")
      /*
      t21.first : Row = [1441735471703,31972
      ,ArrayBuffer([hdisk0,5872], [hdisk1,306], [cd0,0], [hdisk2,943], [hdisk5,6], [hdisk4,3], [hdisk3,931])
      ,ArrayBuffer([aplv,20.000000], [datalv,45.000000], [dumplv01,0.000000], [fslv00,74.000000], [fslv01,0.000000], [hd1,0.000000], [hd10opt,5.000000], [hd2,71.000000], [hd3,32.000000], [hd4,10.000000], [hd5,0.000000], [hd6,0.000000], [hd8,0.000000], [hd9var,70.000000], [lg_dumplv,0.000000], [loglv02,0.000000], [loglv03,0.000000], [paging00,0.000000])
      ,ArrayBuffer([/,10.000000], [/home,18.000000], [/oldhome,61.000000], [/opt,5.000000], [/proc,0.000000], [/sw,74.000000], [/tmp,32.000000], [/u01,20.000000], [/u02,45.000000], [/usr,71.000000], [/var,70.000000])]
       */
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
      /*
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
       */
    } catch {
      case e: org.apache.hadoop.mapred.InvalidInputException => System.err.println(e.getMessage)
    }
  }
}
