package com.edu.spark.log

import java.sql.Timestamp

import com.edu.spark.log.ReaderObject.LogDataFrameReader
import org.apache.spark.sql.SparkSession

object TestDataSource {
  def main(args: Array[String]): Unit = {
    //println(new LogParser("2017-02-09T00:09:27  com.github.michalsenkyr.example.parser.ParserTester  INFO  Started parsing").Line.run())
    val spark = SparkSession.builder()
      .appName("SparkSessionApp")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    /*new LogRelation(spark.sqlContext, "/Users/renren/Downloads/logs").buildScan(Array("dateTime", "application", "level", "message"),
      Array(EqualTo("application", "web"), GreaterThan("dateTime", Timestamp.valueOf("2017-02-08 22:55:00")), LessThan("dateTime", Timestamp.valueOf("2017-02-08 23:05:00"))))
      .foreach(println)*/
    spark.read.log("/Users/renren/Downloads/logs").where('dateTime >= Timestamp.valueOf("2017-02-08 22:55:00") && 'dateTime < Timestamp.valueOf("2017-02-08 23:05:00") && 'application === "web").show()
  }
}
