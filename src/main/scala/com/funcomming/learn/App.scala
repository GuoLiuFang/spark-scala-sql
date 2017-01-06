package com.funcomming.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ${user.name}
  */
object App {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    println("Hello World!")
    println("concat arguments = " + foo(args))
    val conf = new SparkConf().setMaster("local[*]").setAppName("localtest")
    //    val conf = new SparkConf().setMaster("spark://killsong:7077").setAppName("standalonetest")
    //    val conf = new SparkConf().setMaster("spark://NY-HADOOP-12-151:7777").setAppName("线上test")
    val sc = new SparkContext(conf)
    val sparkvar = SparkSession.builder().getOrCreate()
    //暂时没有搞清楚这是做什么用的。。。。
    import sparkvar.implicits._
    //    val gpdf = sparkvar.read.format("jdbc").option("url", "jdbc:postgresql://192.168.12.14:5432/tjdw").option("dbtable", "BDL.crab_code_histories").option("user", "tj_root").option("password", "77pbV1YU!T").load()
    //    gpdf.printSchema()
    //    val mysqldf = sparkvar.read.format("jdbc").option("url", "jdbc:mysql://192.168.12.155:5209/honeycomb").option("dbtable", "sms_received_histories_all_thread").option("user", "guoliufang").option("password", "tiger2108").load()
    //    mysqldf.printSchema()
    //    println("--------------start读取原生文件start-----------")
    //    val distFile = sc.textFile("hdfs://192.168.12.151:8020/guoliufang_test/sql_test/sqllog.txt")
    //    println(distFile.first())
    //    val totalLength = distFile.map(s => s.length).reduce((a, b) => a + b)
    //    println(totalLength)
    //    println("--------------end读取原生文件end-----------")
    //    println("--------------start读取hdfs文件并解析csv文件start-----------")
    //    val realpart00 = sparkvar.read.format("csv").option("sep", "|").load("hdfs://192.168.12.151:8020/user/guoliufang/honeysms/part-00000")
    //    println(realpart00.first())
    //    println(realpart00.printSchema())
    //    val realpart00first = realpart00.take(10)
    //    val array = realpart00first.map(line => line(3))
    //    array.foreach(s => println(s))
    //    println("--------------end读取hdfs文件并解析csv文件end-----------")
    //    println("---------------start文件读取并解析start-----------------------")
    //    val dataFrame = sparkvar.read.json("hdfs://192.168.12.151:8020/guoliufang_test/sql_test/sqllog.txt")
    //    dataFrame.printSchema()
    //    dataFrame.show()
    //    dataFrame.select("createTime").show()
    //    println("-------解析嵌套---------")
    //    dataFrame.select("reasons.serverLog").show()
    //    println("---------------end文件读取并解析end-----------------------")
    println("---------------start改写Python程序的开始start-----------------------")
    val sp_channels_df = sparkvar.read.format("jdbc").option("url", "jdbc:mysql://192.168.12.66:3306/TigerReport_production").option("dbtable", "sp_channels").option("user", "tigerreport").option("password", "titmds4sp").load()
    val charge_codes_df = sparkvar.read.format("jdbc").option("url", "jdbc:mysql://192.168.12.66:3306/TigerReport_production").option("dbtable", "charge_codes").option("user", "tigerreport").option("password", "titmds4sp").load()
    val rows = sp_channels_df.select("id", "name").collect()
    val broadcast = sc.broadcast(rows)
    println("---start看看广播变量中是什么内容start-----")
    broadcast.value.foreach(s => println(s(0), "id---name", s(1)))
    println("---end看看广播变量中是什么内容end-----")
    val tempView = sp_channels_df.createOrReplaceTempView("testspchannels")
    val sp_channelDF = sparkvar.sql("select id, name from testspchannels")
    sp_channelDF.show()
    println("--------------start读取hdfs文件并解析csv文件start-----------")
    val realpart00 = sparkvar.read.format("csv").option("sep", "|").load("hdfs://192.168.12.151:8020/user/guoliufang/scalahoneycombinput/2017-01-05.txt")
    println(realpart00.first())
    println(realpart00.printSchema())
    val realpart00first = realpart00.take(10)
    val array = realpart00first.map(line => line(2))
    array.foreach(s => println(s))
    println("--------------end读取hdfs文件并解析csv文件end-----------")
    println("---------------end改写Python程序的开始end-----------------------")


  }

}
