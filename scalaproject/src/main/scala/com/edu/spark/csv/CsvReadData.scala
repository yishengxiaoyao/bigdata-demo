package com.edu.spark.csv

import org.apache.spark.sql.{SQLContext, SparkSession}

object CsvReadData {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("CustomTextApp")
      .master("local[2]")
      .getOrCreate()
    val sqlContext = new SQLContext(sparkSession.sparkContext)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("/Users/renren/Downloads/cars.csv")
    import sparkSession.implicits._
    val selectedData = df.select("year", "model").filter('year > 2000)
    selectedData.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("newcars.csv")

  }
}
