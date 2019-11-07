package com.edu.spark.sql

import org.apache.spark.{SparkConf, SparkContext}

object PersistenceApp {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf().setAppName("PersistenceApp").setMaster("local[2]")
    var sc = new SparkContext(sparkConf)
    var data = sc.textFile("/Users/renren/Downloads/page_views.dat")
    data.cache()
  }
}
