package com.book.code.ml.transformation

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object StopWordsRemoverExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StopWordsRemoverExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    println("current stop words:")
    println(remover.getStopWords)

    val dataSet = sqlContext.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    println("stop words remove")
    remover.transform(dataSet).show()

    println("stop words remove after add some words")
    remover.setStopWords(remover.getStopWords ++ Array[String]("red"))
    remover.transform(dataSet).show()

    sc.stop()
  }
}