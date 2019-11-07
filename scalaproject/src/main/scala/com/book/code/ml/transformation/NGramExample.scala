package com.book.code.ml.transformation

import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object NGramExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NGramExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val wordDataFrame = sqlContext.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("label", "words")

    val ngram = new NGram().
      setInputCol("words").
      setOutputCol("ngrams").
      setN(3) // 设置 n-gram 的 n 为 3

    val ngramDataFrame = ngram.transform(wordDataFrame)

    ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)

    sc.stop()
  }
}