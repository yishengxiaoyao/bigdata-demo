package com.allaboutscala.chapter12

import org.apache.spark.sql.Dataset


object DataFrameOperationApp extends App with Context {
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss ZZ")
    .csv("file:///Users/renren/IdeaProjects/scalaproject/src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")
  //dfTags.show(10)
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep",",")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("file:///Users/renren/IdeaProjects/scalaproject/src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV
    .filter("score > 400 and score < 410")
    .join(dfTags,"id")
    .select("owner_userid", "tag", "creation_date", "score")

  //dfQuestions.show(10)

  /**
    * convert DataFrame row to scala case class
    */
  import sparkSession.implicits._
  //将数据和类形成一个映射,注意产生的df的列和类的列一只
  val dfTagsOfTag: Dataset[Tag] = dfTags.as[Tag]

  //dfTagsOfTag.take(10).foreach(t => println(s"id = ${t.id},tag = ${t.tag}"))

  /**
    * dataframe row to scala case class using map()
    */
  def toQuestion(row: org.apache.spark.sql.Row): Question = {
    // to normalize our owner_userid data
    /**
      * 将字段的值转换为Int类型
      */
    val IntOf: String => Option[Int] = _ match {
      case s if s == "NA" => None
      case s => Some(s.toInt)
    }

    import java.time._
    /**
      * 将数据变成TimeStamp类型
      */
    val DateOf: String => java.sql.Timestamp = _ match {
      case s => java.sql.Timestamp.valueOf(ZonedDateTime.parse(s).toLocalDateTime)
    }

    Question (
      owner_userid = IntOf(row.getString(0)).getOrElse(-1),
      tag = row.getString(1),
      creationDate = DateOf(row.get(2).toString),
      score = row.getString(3).toInt
    )
  }

  // now let's convert each row into a Question case class
  import sparkSession.implicits._
  val dfOfQuestion: Dataset[Question] = dfQuestions.map(row => toQuestion(row))
  dfOfQuestion
    .take(10)
    .foreach(q => println(s"owner userid = ${q.owner_userid}, tag = ${q.tag}, creation date = ${q.creationDate}, score = ${q.score}"))


  /**
    * create dataframe from collection
    */

  val seqTags = Seq(
    1 -> "so_java",
    1 -> "so_jsp",
    2 -> "so_erlang",
    3 -> "so_scala",
    3 -> "so_akka"
  )


  val dfMoreTags = seqTags.toDF("id", "tag")
  dfMoreTags.show(10)

  /**
    * DataFrame union
    */

  val dfUnionOfTags = dfTags.union(dfMoreTags).filter("id in (1,3)")
  dfUnionOfTags.show(10)

  /**
    * DataFrame Intersection
    */

  val dfIntersectionTags = dfMoreTags
    .intersect(dfUnionOfTags)
    .show(10)

  /**
    * 添加column
    */
    import org.apache.spark.sql.functions._
  val dfSplitColumn = dfMoreTags
    .withColumn("tmp", split($"tag", "_"))
    .select(
      $"id",
      $"tag",
      $"tmp".getItem(0).as("so_prefix"),
      $"tmp".getItem(1).as("so_tag")
    ).drop("tmp")
  dfSplitColumn.show(10)

  sparkSession.stop()

}

case class Tag(id:Int,tag:String)

case class Question(owner_userid: Int, tag: String, creationDate: java.sql.Timestamp, score: Int)