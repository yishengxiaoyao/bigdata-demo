package com.book.code.ml.extraction

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object CountVectorizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CountVectorizerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a")),
      (2, Array("d", "f", "a")),
      (3, Array("d", "c")),
      (4, Array("e"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3) // 选择 top n
      .setMinDF(2) // 设置下限
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).select("features").show()

    sc.stop()
  }
}