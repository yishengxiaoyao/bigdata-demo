package com.book.code.ml.selection

import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ChiSqSelectorExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ChiSqSelectorExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )

    import sqlContext.implicits._

    // 将 data 转化为 DataFrame
    val df = sc.parallelize(data).toDF("id", "features", "clicked")

    val selector = new ChiSqSelector()
      .setNumTopFeatures(1) // 设置输出的 top features
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)
    result.show()

    sc.stop()
  }
}