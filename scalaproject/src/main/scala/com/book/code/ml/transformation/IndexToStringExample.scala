package com.book.code.ml.transformation

import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object IndexToStringExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IndexToStringExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    // 构建将编码转化为 value 的过程，然后进行转换
    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)
    converted.select("id", "originalCategory").show()

    sc.stop()
  }
}