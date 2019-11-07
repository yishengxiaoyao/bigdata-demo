package com.edu.spark.catalog

import org.apache.spark.sql.SparkSession

object CataLogApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("DataSourceAPIApp")
      .master("local[2]")
      .getOrCreate()
    val df = sparkSession.read.format("json").option("path", "/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").load()
    df.write.csv("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/")
    //val df=sparkSession.read.format("csv").option("header",true).option("sep", ";").option("inferSchema", "true").load("")
  }
}
