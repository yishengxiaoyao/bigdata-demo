package com.allaboutscala.chapter12

object FormatColumnApp extends App with Context {
  val donuts = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
  val df = sparkSession.createDataFrame(donuts).toDF("Donut Name", "Price", "Purchase Date")

  import org.apache.spark.sql.functions._
  import sparkSession.sqlContext.implicits._

  /**
    * 将列数据进行格式化
    */
  df
    .withColumn("Price Formatted", format_number($"Price", 2))
    .withColumn("Name Formatted", format_string("awesome %s", $"Donut Name"))
    .withColumn("Name Uppercase", upper($"Donut Name"))
    .withColumn("Name Lowercase", lower($"Donut Name"))
    .withColumn("Date Formatted", date_format($"Purchase Date", "yyyyMMdd"))
    .withColumn("Day", dayofmonth($"Purchase Date"))
    .withColumn("Month", month($"Purchase Date"))
    .withColumn("Year", year($"Purchase Date"))
    .show()
}
