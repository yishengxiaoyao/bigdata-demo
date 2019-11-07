package com.edu.spark.v1

import com.edu.spark.v1.util.DateUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object LogParser {

  // ETL: 字段拆分、字段补充

  // schema
  val struct = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("salary", DoubleType),
    StructField("time", LongType),
    StructField("day", StringType),  //分区字段
    StructField("hour", StringType)   //分区字段
  ))

  def parseLog(log: String): Row = {
    try {
      // 	1	name1	2019-04-05 10:14:09	5870
      val splits = log.split("\t")
      val id = splits(0).toInt
      val name = splits(1)
      val salary = splits(3).toDouble

      val time = DateUtils.getTime(splits(2))
      val minute = DateUtils.parseToMinute(splits(2))
      val day = DateUtils.getDay(minute)
      val hour = DateUtils.getHour(minute)

      Row(
        id, name,salary, time, day, hour
      )
    } catch {
      case e: Exception => e.printStackTrace()
        Row(0)
    }
  }
}
