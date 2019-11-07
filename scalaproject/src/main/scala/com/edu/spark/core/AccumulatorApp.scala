package com.edu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("AccumulatorApp")
    val sc = new SparkContext(sparkConf)
    val count = sc.longAccumulator
    sc.parallelize(Array(1, 2, 3, 4)).collect().foreach(x => count.add(x))
    println(count.value)
  }
}
