package com.allaboutscala.chapter12

object FirstRowApp extends App with Context {
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")
  /**
    * 第一行数据
    */
  val firstRow = df.first()
  println(s"First row = $firstRow")
  /**
    * 第一行数据的第几个元素
    */
  val firstRowColumn1 = df.first().get(0)
  println(s"First row column 1 = $firstRowColumn1")

  val firstRowColumnPrice = df.first().getAs[Double]("Price")
  println(s"First row column Price = $firstRowColumnPrice")
}
