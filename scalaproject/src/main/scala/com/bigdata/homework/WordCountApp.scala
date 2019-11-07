package com.bigdata.homework

import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp {
  def main(args: Array[String]): Unit = {
    argsFunction(args)
  }

  def argsFunction(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(args(0))
    val spark = new SparkContext(sparkConf)
    val data = spark.textFile(args(1))
    val words = data.flatMap(x => x.split(" "))
    val wordone = words.map(x => (x, 1))
    val result = wordone.reduceByKey(_ + _)
    //简写形式
    //val result=spark.textFile(args(1)).flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    result.collect().foreach(println)
  }

  def noArgs(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountApp")
    val spark = new SparkContext(sparkConf)
    val data = spark.textFile("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/data.txt")
    val words = data.flatMap(x => x.split(" "))
    val wordone = words.map(x => (x, 1))
    val result = wordone.reduceByKey(_ + _)
    result.collect().foreach(println)
  }
}
