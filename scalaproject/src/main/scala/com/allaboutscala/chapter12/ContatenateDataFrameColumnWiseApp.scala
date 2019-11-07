package com.allaboutscala.chapter12

import org.apache.spark.sql.functions._

object ContatenateDataFrameColumnWiseApp extends App with Context {

  val donuts = Seq(("111","plain donut", 1.50), ("222", "vanilla donut", 2.0), ("333","glazed donut", 2.50))

  val dfDonuts = sparkSession
    .createDataFrame(donuts)
    .toDF("Id","Donut Name", "Price")
  dfDonuts.show()

  val inventory = Seq(("111", 10), ("222", 20), ("333", 30))
  val dfInventory = sparkSession
    .createDataFrame(inventory)
    .toDF("Id", "Inventory")
  dfInventory.show()
  /**
    * 执行内关联
    */
  val dfDonutsInventory = dfDonuts.join(dfInventory, Seq("Id"), "inner")
  dfDonutsInventory.show()

  /**
    * Search DataFrame column using array_contains()
    */
  import sparkSession.sqlContext.implicits._

  val tagsDF = sparkSession
    .read
    .option("multiLine", true)
    .option("inferSchema", true)
    .json("/Users/renren/IdeaProjects/scalaproject/src/main/resources/tags_sample.json")

  val df = tagsDF
    .select(explode($"stackoverflow") as "stackoverflow_tags")
    .select(
      $"stackoverflow_tags.tag.id" as "id",
      $"stackoverflow_tags.tag.author" as "author",
      $"stackoverflow_tags.tag.name" as "tag_name",
      $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
      $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
    )
  df.show()

  /**
    * 某一列中是否含有某个值
    */

  df.select("*")
    .where(array_contains($"frameworks_name","Play Framework"))
    .show()
  /**
    * Check DataFrame column exists
    * 是否存在这一列
    */
  val priceColumnExists = dfDonuts.columns.contains("Price")
  println(s"Does price column exist = $priceColumnExists")


}
