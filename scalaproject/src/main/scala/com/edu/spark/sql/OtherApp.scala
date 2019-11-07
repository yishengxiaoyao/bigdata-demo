package com.edu.spark.sql

import org.apache.spark.sql.SparkSession

object OtherApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SparkSessionApp")
      .master("local[2]")
      .getOrCreate()
    val catalog = sparkSession.catalog
    catalog.listDatabases().show() //输出所有的数据库
    catalog.listTables("default").show() //输出数据库的表
    catalog.listColumns("default", "emp").show() //输出表的列名
    catalog.listFunctions().show() //输出所有的方法
    import sparkSession.implicits._
    val df = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").csv("")
    val ds = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").csv("").as[Sales]

    //将运行时异常调整为编译时异常,使用ds编程,输出某一项。

    sparkSession.stop()
  }

  case class Sales(transactionId: Int, customerId: Int, itermId: Int, amountPaid: Double)

}

