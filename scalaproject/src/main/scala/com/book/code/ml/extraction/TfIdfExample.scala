package com.book.code.ml.extraction

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TfIdfExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TfIdfExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    // sentence => words 分词
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    // words => rawFeatures，原始的特征
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(200)
    val featurizedData = hashingTF.transform(wordsData)

    // 对原始特征的一个转换，log(n/d), d 表示在多少个文档中出现了，这里实际上还做了一个平滑，为 log((n+1)/(d+1))，注意以 E 为底
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    // 特征选择
    rescaledData.select("features", "label").take(3).foreach(println)

    sc.stop()
  }
}