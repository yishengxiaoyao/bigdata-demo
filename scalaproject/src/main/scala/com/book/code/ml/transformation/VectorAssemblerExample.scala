package com.book.code.ml.transformation

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object VectorAssemblerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("VectorAssemblerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dataset = sqlContext.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    // 进行一个向量的合并，对 hour，mobile，userFeatures 进行合并，最终合并为了一个 features
    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(dataset)

    println(output.select("features", "clicked").first())

    sc.stop()
  }
}