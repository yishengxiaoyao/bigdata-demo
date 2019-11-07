package com.book.code.ml.transformation

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object StringIndexerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StringIndexerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 创建 df
    val df = sqlContext.createDataFrame(
      Seq((0, "a"),
        (1, "b"),
        (2, "c"),
        (3, "a"),
        (4, "a"),
        (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    // 对 category 字段进行编码，fit 相当于是训练拟合的过程，transform 是一个转换的过程
    val indexed = indexer.fit(df).transform(df)

    // 显示转换后的结果
    indexed.show()

    sc.stop()
  }
}