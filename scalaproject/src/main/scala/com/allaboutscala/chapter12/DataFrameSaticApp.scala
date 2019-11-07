package com.allaboutscala.chapter12

import org.apache.spark.sql.functions._


object DataFrameSaticApp extends App with Context {

  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss ZZ")
    .csv("file:///Users/renren/IdeaProjects/scalaproject/src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("file:///Users/renren/IdeaProjects/scalaproject/src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )
  /**
    * 某一列的平均值
    */
  //dfQuestions.select(avg("score")).show()

  /**
    * 某一列的最大值
    */
  //dfQuestions.select(max("socre")).show()

  /**
    * 某一列的最小值
    */
  //dfQuestions.select(min("socre")).show()

  /**
    *
    */
  //dfQuestions.select(mean("socre")).show()

  /**
    * 某一列求和
    */
  //dfQuestions.select(sum("socre")).show()

  /**
    * 分组
    */
  /*dfQuestions
    .filter("id > 400 and id < 450")
    .filter("owner_userid is not null")
    .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
    .groupBy(dfQuestions.col("owner_userid"))
    .agg(avg("score"), max("answer_count"))
    .show()*/
  /**
    * describe方法:可以输出count、mean、standard deviation、min和max。
    */
  val dfQuestionsStatistics = dfQuestions.describe()
  dfQuestionsStatistics.show()

  // Correlation:关联,需要有stat(DataFrameStatFunctions)对象,找到score和answer_count这两列之间的关系
  val correlation = dfQuestions.stat.corr("score", "answer_count")
  println(s"correlation between column score and answer_count = $correlation")


  // Covariance:协方差,score与answer_count之间的协方差
  val covariance = dfQuestions.stat.cov("score", "answer_count")
  println(s"covariance between column score and answer_count = $covariance")


  // Frequent Items:常用的列
  val dfFrequentScore = dfQuestions.stat.freqItems(Seq("answer_count"))
  dfFrequentScore.show()


  // Crosstab:
  val dfScoreByUserid = dfQuestions
    .filter("owner_userid > 0 and owner_userid < 20")
    .stat
    .crosstab("score", "owner_userid")
  dfScoreByUserid.show(10)


  // Stratified sampling using sampleBy
  // find all rows where answer_count in (5, 10, 20)
  val dfQuestionsByAnswerCount = dfQuestions
    .filter("owner_userid > 0")
    .filter("answer_count in (5, 10, 20)")

  // count how many rows match answer_count in (5, 10, 20)
  dfQuestionsByAnswerCount
    .groupBy("answer_count")
    .count()
    .show()

  //获取这个值对应数据集的数据量，例如 answer_count 为5时，总的数据量为811，下面在取样的情况下，就是取总量的50%。
  // Create a fraction map where we are only interested:
  // - 50% of the rows that have answer_count = 5
  // - 10% of the rows that have answer_count = 10
  // - 100% of the rows that have answer_count = 20
  // Note also that fractions should be in the range [0, 1]
  val fractionKeyMap = Map(5 -> 0.5, 10 -> 0.1, 20 -> 1.0)

  // Stratified sample using the fractionKeyMap.
  dfQuestionsByAnswerCount
    .stat
    .sampleBy("answer_count", fractionKeyMap, 7L)
    .groupBy("answer_count")
    .count()
    .show()

  // Note that changing the random seed will modify your sampling outcome. As an example, let's change the random seed to 37.
  dfQuestionsByAnswerCount
    .stat
    .sampleBy("answer_count", fractionKeyMap, 37L)
    .groupBy("answer_count")
    .count()
    .show()


  // Approximate Quantile:近似分位数
  val quantiles = dfQuestions
    .stat
    .approxQuantile("score", Array(0, 0.5, 1), 0.25)
  println(s"Qauntiles segments = ${quantiles.toSeq}")


  // You can verify the quantiles statistics above using Spark SQL as follows:
  dfQuestions.createOrReplaceTempView("so_questions")
  sparkSession
    .sql("select min(score), percentile_approx(score, 0.25), max(score) from so_questions")
    .show()


  // Bloom Filter:具体的列，希望放入的数量，错误率
  val tagsBloomFilter = dfTags.stat.bloomFilter("tag", 1000L, 0.1)
  println(s"bloom filter contains java tag = ${tagsBloomFilter.mightContain("java")}")
  println(s"bloom filter contains some unknown tag = ${tagsBloomFilter.mightContain("unknown tag")}")


  // Count Min Sketch
  val cmsTag = dfTags.stat.countMinSketch("tag", 0.1, 0.9, 37)
  val estimatedFrequency = cmsTag.estimateCount("java")
  println(s"Estimated frequency for tag java = $estimatedFrequency")


  // Sampling With Replacement
  val dfTagsSample = dfTags.sample(true, 0.2, 37L)
  println(s"Number of rows in sample dfTagsSample = ${dfTagsSample.count()}")
  println(s"Number of rows in dfTags = ${dfTags.count()}")

  sparkSession.stop()

}
