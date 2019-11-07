package com.allaboutscala.chapter12

object DropNullApp extends App with Context {
  val donuts = Seq(("plain donut", 1.50), (null.asInstanceOf[String], 2.0), ("glazed donut", 2.50))
  val dfWithNull = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")
  /**
    * 去掉null的行
    */
  dfWithNull.show()
  val dfWithoutNull = dfWithNull.na.drop()
  dfWithoutNull.show()

}
