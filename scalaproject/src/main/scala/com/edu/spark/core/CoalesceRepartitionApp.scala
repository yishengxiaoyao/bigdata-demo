package com.edu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object CoalesceRepartitionApp {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("CoalesceRepartitionApp")
    val sc = new SparkContext(sparkConf)

    val students = new ListBuffer[String]()
    for (i <- 1 to 100) {
      students += "student：" + i
    }

    val stus = sc.parallelize(students, 3)
    //获取partition的数量
    println(stus.partitions.size)
    stus.mapPartitionsWithIndex((index, partition) => {
      val emps = new ListBuffer[String]
      while (partition.hasNext) {
        emps += ("~~~" + partition.next() + " ,old:[" + (index + 1) + "]")
      }
      emps.iterator
    }).foreach(println)

    println("============coalesce================")
    stus.coalesce(2).mapPartitionsWithIndex((index, partition) => {
      val emps = new ListBuffer[String]
      while (partition.hasNext) {
        emps += ("~~~" + partition.next() + " , new:[" + (index + 1) + "]")
      }
      emps.iterator
    }).foreach(println)
    println("============repartition================")

    stus.repartition(5).mapPartitionsWithIndex((index, partition) => {
      val emps = new ListBuffer[String]
      while (partition.hasNext) {
        emps += ("~~~" + partition.next() + " , new:[" + (index + 1) + "]")
      }
      emps.iterator
    }).foreach(println)

    sc.stop()
  }

}
