package com.book.code.ml.cluster

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object KMeansExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Crates a DataFrame
    val dataset: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0, 0.0)),
      (2, Vectors.dense(0.1, 0.1, 0.1)),
      (3, Vectors.dense(0.2, 0.2, 0.2)),
      (4, Vectors.dense(9.0, 9.0, 9.0)),
      (5, Vectors.dense(9.1, 9.1, 9.1)),
      (6, Vectors.dense(9.2, 9.2, 9.2))
    )).toDF("id", "features")

    // Trains a k-means model
    val kmeans = new KMeans()
      .setK(2)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    // 生成模型
    val model = kmeans.fit(dataset)

    // Shows the result
    println("Final Centers: ")
    model.clusterCenters.foreach(println)

    println("Show cluster results: ")
    model.transform(dataset).show()

    sc.stop()
  }
}