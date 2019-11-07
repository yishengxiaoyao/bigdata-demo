package com.allaboutscala.chapter12

object SparkSqlApp extends App with Context {
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss ZZ")
    .csv("file:///Users/renren/IdeaProjects/scalaproject/src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.createOrReplaceTempView("so_tags")
  /**
    * 从catalog中列出所有的数据表
    */
  //sparkSession.catalog.listTables().show()
  //sparkSession.sql("show tables").show()

  /**
    * 选择列
    */
  //sparkSession.sql(" select id,tag from so_tags limit 10").show()

  /**
    * filter
    */
  //sparkSession.sql(" select id,tag from so_tags where tag = 'php'").show(10)
  /**
    * 统计列
    */
  /*sparkSession.sql(
    """
      |select
      |count(*) as php_count
      |from so_tags where tag='php'
    """.stripMargin).show(10)*/
  /**
    * sql like
    */
  /*sparkSession.sql(
    """
      |select * from so_tags
      |where tag like 's%'
    """.stripMargin).show(10)*/

  /**
    * where 语句
    */

  /*sparkSession.sql(
    """
      | select * from so_tags
      | where tag like 's%'
      | and (id = 25 or id = 108)
    """.stripMargin).show(10)*/

  /**
    * in clause
    */

  /*parkSession
    .sql(
      """select *
        |from so_tags
        |where id in (25, 108)""".stripMargin)
    .show(10)*/

  /**
    * group by having
    */

  /*sparkSession
    .sql(
      """select tag, count(*) as count
        |from so_tags group by tag""".stripMargin)
    .show(10)*/

  /*sparkSession
    .sql(
      """select tag, count(*) as count
        |from so_tags group by tag having count > 5""".stripMargin)
    .show(10)*/

  /**
    * order by
    */

  /*sparkSession
    .sql(
      """select tag, count(*) as count
        |from so_tags group by tag having count > 5 order by tag""".stripMargin)
    .show(10)*/

  /**
    *
    */
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
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

  val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410 ").toDF()

  dfQuestionsSubset.createOrReplaceTempView("so_questions")

  /**
    * inner join
    */
  sparkSession
    .sql(
      """select t.*, q.*
        |from so_questions q
        |inner join so_tags t
        |on t.id = q.id""".stripMargin)
    .show(10,false)

  // SQL Left Outer Join
  sparkSession
    .sql(
      """
        |select t.*, q.*
        |from so_questions q
        |left outer join so_tags t
        |on t.id = q.id
      """.stripMargin)
    .show(10)

  //  dfQuestionsSubset
  //    .join(dfTags, Seq("id"), "left_outer")
  //    .show(10)



  // SQL Right Outer Join
  sparkSession
    .sql(
      """
        |select t.*, q.*
        |from so_tags t
        |right outer join so_questions q
        |on t.id = q.id
      """.stripMargin)
    .show(10)

  //  dfTags
  //    .join(dfQuestionsSubset, Seq("id"), "right_outer")
  //    .show(10)


  // Register User Defined Function (UDF)
  // Function to prefix a String with so_ short for StackOverflow
  def prefixStackoverflow(s: String): String = s"so_$s"

  // Register User Defined Function (UDF)
  sparkSession
    .udf
    .register("prefix_so", prefixStackoverflow _)

  // Use udf prefix_so to augment each tag value with so_
  sparkSession
    .sql("""select id, prefix_so(tag)
           |from so_tags""".stripMargin)
    .show(10)


  // SQL Distinct
  sparkSession
    .sql("""select distinct tag
           |from so_tags""".stripMargin)
    .show(10)


  sparkSession.stop()
}
