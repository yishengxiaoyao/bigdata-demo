package com.allaboutscala.chapter12

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkConf = new SparkConf()
          .setAppName("learn spark")
          .setMaster("local[*]")
          .set("spark.core.max","2")

  lazy val sparkSession = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()

}
