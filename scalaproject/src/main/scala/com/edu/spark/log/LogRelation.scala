package com.edu.spark.log

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SQLContext}

class LogRelation(val sqlContext: SQLContext, val path: String) extends BaseRelation with PrunedFilteredScan {
  override def schema: StructType = StructType(Seq(
    StructField("application", StringType, false),
    StructField("dateTime", TimestampType, false),
    StructField("component", StringType, false),
    StructField("level", StringType, false),
    StructField("message", StringType, false)))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val (application, from, to) = filters
      .foldLeft((Option.empty[String], Option.empty[Timestamp], Option.empty[Timestamp])) {
        case (x, EqualTo("application", value: String)) => x.copy(_1 = Some(value))
        case (x, GreaterThan("dateTime", value: Timestamp)) => x.copy(_2 = Some(value))
        case (x, GreaterThanOrEqual("dateTime", value: Timestamp)) => x.copy(_2 = Some(value))
        case (x, LessThan("dateTime", value: Timestamp)) => x.copy(_3 = Some(value))
        case (x, LessThanOrEqual("dateTime", value: Timestamp)) => x.copy(_3 = Some(value))
        case (x, _) => x
      }
    if (application.isEmpty) println("Application not specified")
    if (from.isEmpty) println("Timestamp lower bound not specified")
    val (fromLimit, toLimit) = (
      from.get.toLocalDateTime.toLocalDate,
      to.map(_.toLocalDateTime.toLocalDate).getOrElse(LocalDate.now)
    )
    val pathPattern =
      Iterator.iterate(fromLimit)(_.plusDays(1))
        .takeWhile(!_.isAfter(toLimit))
        .map(_.format(DateTimeFormatter.ISO_LOCAL_DATE))
        .mkString(s"$path/${application.get}/{", ",", "}.gz")

    sqlContext.sparkContext.textFile(pathPattern).map { line =>
      val record = new LogParser(line).Line.run().get
      Row(application.get, record.dateTime, record.component, record.level, record.message)
    }
  }

}