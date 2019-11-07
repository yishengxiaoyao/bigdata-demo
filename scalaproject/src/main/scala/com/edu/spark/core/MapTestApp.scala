package com.edu.spark.core

import org.apache.spark.sql.SparkSession

object MapTestApp {

  def main(args: Array[String]): Unit = {
    maptest()
    //flatmaptest()
  }

  def maptest(): Unit = {
    val spark = SparkSession.builder.appName("mapExample").master("local").getOrCreate()
    val data = spark.read.textFile("/Users/renren/Downloads/spark_test.txt").rdd
    val mapFile = data.map(line => line.split(" "))
    mapFile.foreach(println)
  }

  def flatmaptest(): Unit = {
    val spark = SparkSession.builder.appName("mapExample").master("local").getOrCreate()
    val data = spark.read.textFile("/Users/renren/Downloads/spark_test.txt").rdd
    val flatmapFile = data.flatMap(lines => lines.split(" "))
    flatmapFile.foreach(println)
  }

}
