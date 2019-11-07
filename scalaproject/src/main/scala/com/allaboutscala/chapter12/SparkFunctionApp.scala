package com.allaboutscala.chapter12

object SparkFunctionApp extends App with Context {
  /**
    * create dataframe from tuples
    */
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  //df.show()

  /**
    * get dataframe column names
    */
  val columnNames:Array[String] = df.columns

  //columnNames.foreach(name=>println(s"$name"))

  /**
    * dataframe column names and types
    *
    * dtypes: 返回列名和列的类型
    */

  val (columnName, columnDataTypes) = df.dtypes.unzip

  //println(s"DataFrame column names = ${columnNames.mkString(", ")}")
  //println(s"DataFrame column data types = ${columnDataTypes.mkString(", ")}")

  /**
    * json into DataFrame using explode()
    *
    * explode()展开root元素，并且可以起别名
    */
  import sparkSession.sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val tagsDF = sparkSession
    .read
    .option("multiLine", true)
    .option("inferSchema", true)
    .json("/Users/renren/IdeaProjects/scalaproject/src/main/resources/tags_sample.json")

  val jsonDf = tagsDF.select(explode($"stackoverflow") as "stackoverflow_tags")

  jsonDf.printSchema()

  jsonDf.select(
    $"stackoverflow_tags.tag.id" as "id",
    $"stackoverflow_tags.tag.author" as "author",
    $"stackoverflow_tags.tag.name" as "tag_name",
    $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
    $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
  ).show()

}
