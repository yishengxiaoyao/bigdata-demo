package com.edu.spark.stream

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object BlackListApp {
  def main(args: Array[String]): Unit = {

  }

  def blackList(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("BlackListApp")
    val sc = new SparkContext(sparkConf)
    //1.名字，2.info
    val input = new ListBuffer[(String, String)]
    input.append(("yishengxiaoyao", "yishengxiaoyao info"))
    input.append(("xiaoyaoyisheng", "xiaoyaoyisheng info"))
    input.append(("xiaoyaoshan", "xiaoyaoshan info"))
    input.append(("xiaoyao", "xiaoyao info"))
    //将数据并行变成RDD
    val inputRDD = sc.parallelize(input)
    //黑名单：1.name,2.false
    val blackTuple = new ListBuffer[(String, Boolean)]
    blackTuple.append(("yishengxiaoyao", true))
    blackTuple.append(("xiaoyao", true))
    val blackRdd = sc.parallelize(blackTuple)
    //使用左外连接，如果后面没有数据，设置为null
    inputRDD.leftOuterJoin(blackRdd).filter(x => {
      x._2._2.getOrElse(false) != true
    }).map(_._2._1).collect().foreach(println)
    sc.stop()
  }
}
