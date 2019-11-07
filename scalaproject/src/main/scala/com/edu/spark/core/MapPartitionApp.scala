package com.edu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MapPartitionApp {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("MapPartitionApp")
    val sc = new SparkContext(sparkConf)
    val students = new ListBuffer[String]()
    for (i <- 1 to 100) {
      students += "student：" + i
    }

    /**
      * map操作
      */
    /*val stus = sc.parallelize(students, 4)
    stus.map(x => {
      val connection =  DBUtils.getConnection()
      println(connection + "~~~~~~~~~~~~")
      DBUtils.returnConnection(connection)
    }).foreach(println)*/
    /**
      * mapPartitions操作，需要定义并行度
      */
    val stus = sc.parallelize(students, 4)
    stus.mapPartitions(partition => {
      val connection = DBUtils.getConnection()
      println(connection + "~~~~~~~~~~~~")
      DBUtils.returnConnection(connection)
      partition
    }).foreach(println)
    sc.stop()
  }

}
