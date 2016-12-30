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
    //    val conf = new SparkConf().setMaster("local[*]").setAppName("localtest")
    val conf = new SparkConf().setMaster("spark://killsong:7077").setAppName("localtest")
    //        val conf = new SparkConf().setMaster("spark://NY-HADOOP-12-151:7777").setAppName("线上test")
    val sc = new SparkContext(conf)
    val sparkvar = SparkSession.builder().getOrCreate()
    import sparkvar.implicits._
    val gpdf = sparkvar.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://192.168.12.14:5432/tjdw")
      .option("dbtable", "BDL.crab_code_histories")
      .option("user", "tj_root")
      .option("password", "77pbV1YU!T")
      .load()
    //spark连接数据库，把需要的参考，加载进来。
    gpdf.printSchema()
    val mysqldf = sparkvar.read.format("jdbc").option("url", "jdbc:mysql://192.168.12.155:5209/honeycomb").option("dbtable", "sms_received_histories_all_thread").option("user", "guoliufang").option("password", "tiger2108").load()
    mysqldf.printSchema()
    val distFile = sc.textFile("hdfs://192.168.12.151:8020/guoliufang_test/sql_test/sqllog.txt")
    val dataFrame = sparkvar.read.json("hdfs://192.168.12.151:8020/guoliufang_test/sql_test/sqllog.txt")
    dataFrame.printSchema()
    dataFrame.show()
    dataFrame.select("createTime").show()
    println("-------解析嵌套---------")
    dataFrame.select("reasons.serverLog").show()
    println(distFile.first())
    val totalLength = distFile.map(s => s.length).reduce((a, b) => a + b)
    println(totalLength)
  }

}
