package com.allaboutscala.chapter12

object SplitArrayColumnApp extends App with Context {
  import sparkSession.sqlContext.implicits._

  val targets = Seq(("Plain Donut", Array(1.50, 2.0)), ("Vanilla Donut", Array(2.0, 2.50)), ("Strawberry Donut", Array(2.50, 3.50)))
  val df = sparkSession
    .createDataFrame(targets)
    .toDF("Name", "Prices")

  df.show()

  df.printSchema()
  /**
    * 将一个属性进行切割
    */
  val df2 = df
    .select(
      $"Name",
      $"Prices"(0).as("Low Price"),
      $"Prices"(1).as("High Price")
    )

  df2.show()

  /**
    * 为column命名
    */
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val originalDF = sparkSession.createDataFrame(donuts).toDF("Donut Name", "Price")
  originalDF.show()

  val finalDf = originalDF.withColumnRenamed("Donut Name", "Name")
  finalDf.show()

}
