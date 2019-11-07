package com.book.code.ml.transformation

import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object PCAExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PCAExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )

    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)

    val pcaDF = pca.transform(df)
    val result = pcaDF.select("pcaFeatures")
    result.show()

    sc.stop()
  }
}