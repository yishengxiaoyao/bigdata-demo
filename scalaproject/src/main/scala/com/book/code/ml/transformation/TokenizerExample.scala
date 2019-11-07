package com.book.code.ml.transformation

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TokenizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TokenizerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val sentenceDataFrame = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("label", "sentence")

    // 采用空格分词
    val tokenizer = new Tokenizer().
      setInputCol("sentence").
      setOutputCol("words")

    // \W 是用来匹配任意不是字母，数字，下划线，汉字的字符
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val tokenized = tokenizer.transform(sentenceDataFrame)

    println("result of tokenizer...")
    tokenized.select("words", "label").take(3).foreach(println)

    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)

    println("result of regex tokenizer...")
    regexTokenized.select("words", "label").take(3).foreach(println)

    sc.stop()
  }
}