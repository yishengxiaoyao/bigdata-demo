package com.book.code.ml.transformation

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object VectorIndexerExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: [input] [maxCategories]")
      sys.exit(1)
    }

    val input = args(0)
    val maxCategories = args(1) toInt

    val conf = new SparkConf().setAppName("VectorIndexerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.format("libsvm").load(input)

    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(maxCategories) // 设置最大的 label 出现次数

    // 对 data 进行拟合
    val indexerModel = indexer.fit(data)

    // 有哪些 feature，具体的是 column
    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet

    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    // 建立新的 column "indexed"
    val indexedData = indexerModel.transform(data)

    indexedData.show()

    sc.stop()
  }
}