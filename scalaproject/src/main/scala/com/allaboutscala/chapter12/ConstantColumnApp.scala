package com.allaboutscala.chapter12

import org.apache.spark.sql.functions._

object ConstantColumnApp extends App with Context {
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession.createDataFrame(donuts).toDF("Donut Name", "Price")

  import org.apache.spark.sql.functions._

  /**
    * 创建具体的值，并且指定值lit
    */
  val df2 = df
    .withColumn("Tasty", lit(true))
    .withColumn("Correlation", lit(1))
    .withColumn("Stock Min Max", typedLit(Seq(100, 500)))

  df2.show()

  /**
    * 使用UDF来定义新的column
    */

    import sparkSession.sqlContext.implicits._
  val donutData = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val originalDF = sparkSession.createDataFrame(donuts).toDF("Donut Name", "Price")

  val stockMinMax: (String => Seq[Int]) = (donutName: String) => donutName match {
    case "plain donut"    => Seq(100, 500)
    case "vanilla donut"  => Seq(200, 400)
    case "glazed donut"   => Seq(300, 600)
    case _                => Seq(150, 150)
  }


  val udfStockMinMax = udf(stockMinMax)
  val resultDF = originalDF.withColumn("Stock Min Max", udfStockMinMax($"Donut Name"))
  resultDF.show()

}
