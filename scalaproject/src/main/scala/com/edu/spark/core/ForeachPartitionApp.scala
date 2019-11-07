package com.edu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object ForeachPartitionApp {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("ForeachPartitionApp")
    val sc = new SparkContext(sparkConf)
    val students = new ListBuffer[String]()
    for (i <- 1 to 100) {
      students += "student：" + i
    }
    val stus = sc.parallelize(students, 4)

    /**
      * foreach是对每一个数据进行操作
      */
    stus.foreach(x => {
      val connection = DBUtils.getConnection()
      println(connection + "~~~~~~~~~~~~")
      DBUtils.returnConnection(connection)
    })

    /**
      * foreachPartition 对每个partition进行操作
      */
    stus.foreachPartition(partition => {
      val connection = DBUtils.getConnection()
      println(connection + "~~~~~~~~~~~~")
      DBUtils.returnConnection(connection)
    })
    sc.stop()
  }

}
