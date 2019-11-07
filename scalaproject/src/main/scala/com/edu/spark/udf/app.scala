package com.edu.spark.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object app extends App {
  println("Application started...")

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  val df = spark.sqlContext.read.format("com.edu.spark.udf.datasource").load("/Users/renren/Downloads/data")
  //print the schema
  //  df.printSchema()

  //print the data
  //  df.show()

  //save the data
  //  df.write.options(Map("format" -> "customFormat")).mode(SaveMode.Overwrite).format("com.edu.spark.udf.datasource").save("/Users/renren/Downloads/out_custom/")
  //  df.write.options(Map("format" -> "json")).mode(SaveMode.Overwrite).format("com.edu.spark.udf.datasource").save("/Users/renren/Downloads/out_json/")
  //  df.write.mode(SaveMode.Overwrite).format("com.edu.spark.udf.datasource").save("/Users/renren/Downloads/out_none/")

  //select some specific columns
  //  df.createOrReplaceTempView("test")
  //  spark.sql("select id, name, salary from test").show()

  //filter data
  df.createOrReplaceTempView("test")
  spark.sql("select id,name,gender from test where salary == 50000").write.mode(SaveMode.Overwrite).format("com.edu.spark.udf.datasource").save("/Users/renren/Downloads/data/result")

  println("Application Ended...")
}
