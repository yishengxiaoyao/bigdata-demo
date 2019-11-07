package com.allaboutscala.chapter12

object ColumnHashingApp extends App with Context {
  val donuts = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
  val df = sparkSession.createDataFrame(donuts).toDF("Donut Name", "Price", "Purchase Date")

  import org.apache.spark.sql.functions._
  import sparkSession.sqlContext.implicits._

  /**
    * 计算hash值、md5值、sha1值、sha2值
    */
  df
    .withColumn("Hash", hash($"Donut Name")) // murmur3 hash as default.
    .withColumn("MD5", md5($"Donut Name"))
    .withColumn("SHA1", sha1($"Donut Name"))
    .withColumn("SHA2", sha2($"Donut Name", 256)) // 256 is the number of bits
    .show()
}
