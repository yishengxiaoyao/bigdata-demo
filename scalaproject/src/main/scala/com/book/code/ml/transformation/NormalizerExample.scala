package com.book.code.ml.transformation

import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object NormalizerExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: [input]")
      sys.exit(1)
    }

    val input = args(0)

    val conf = new SparkConf().setAppName("NormalizerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dataFrame = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    // Normalize each Vector using $L^1$ norm.
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val l1NormData = normalizer.transform(dataFrame)

    // 显示 transform 之后的结果
    l1NormData.show()

    // 正无穷大，相当于 value/maxValue
    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
    lInfNormData.show()

    sc.stop()
  }
}