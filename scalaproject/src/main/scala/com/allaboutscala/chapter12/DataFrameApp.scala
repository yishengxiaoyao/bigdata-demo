package com.allaboutscala.chapter12

object DataFrameApp extends App with Context {
  /**
    * 在读取csv文件时,需要设置时间的格式，否则报错Illegal pattern component: XXX
    */
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss ZZ")
    .csv("file:///Users/renren/IdeaProjects/scalaproject/src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")
  /**
    * 输出数据的个数，默认为20
    */
  //dfTags.show(10)
  /**
    * 输出schema信息
    */
  //dfTags.printSchema()

  /**
    * 查询指定的行
    */

  //dfTags.select("id","tag").take(10)

  /**
    * 过滤
    */
  //dfTags.filter("tag == 'php'").show(10)
  /**
    * 统计
    */
  //println(s"Number of php tags = ${ dfTags.filter("tag == 'php'").count() }")

  /**
    * like sql
    */
  //dfTags.filter("tag like 's%'").show(10)

  /**
    * 多个filter
    */
  /*dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)*/
  /**
    * in sql
    */

  //dfTags.filter(" id in (25,108) ").show(10)
  /**
    * group by
    */
  //dfTags.groupBy("tag").count().show(10)

  /**
    * group by with filter
    */
  //dfTags.groupBy("tag").count().filter("count > 5").show(10)

  /**
    * group by with order by
    */
 // import sparkSession.implicits._
 // dfTags.groupBy("tag").count().filter("count > 5").orderBy("tag").sort($"count".desc).show(10)
  /**
    * cast columns to specific type
    */

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("file:///Users/renren/IdeaProjects/scalaproject/src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  //dfQuestionsCSV.printSchema()
  /**
    * 将某个一列转换为相应的类型
    */
  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  //dfQuestions.printSchema()
  //dfQuestions.show(10)

  /**
    * operate on a filtered dataframe
    */
  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410 ").toDF()
  //dfQuestionsSubset.show(10)

  /**
    * inner join  指定join的字段
    */
  //dfQuestionsSubset.join(dfTags,"id").show(10)

  /**
    * join select
    */
  /*dfQuestionsSubset
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .show(10)*/

  /**
    * join on explicit columns:可以指定具体的列
    */
  /*dfQuestionsSubset
    .join(dfTags, dfTags("id") === dfQuestionsSubset("id"))
    .show(10)*/

  /**
    * inner join  可以指定join的类型
    * Type of join to perform. Default `inner`. Must be one of:`inner`, `cross`,
    * `outer`, `full`, `full_outer`, `left`, `left_outer`,`right`,
    * `right_outer`, `left_semi`, `left_anti`.
    */
  /*dfQuestionsSubset
    .join(dfTags, Seq("id"), "inner")
    .show(10)*/
  /**
    * distinct
    */
  dfTags
    .select("tag")
    .distinct()
    .show(10)
  sparkSession.stop()
}
