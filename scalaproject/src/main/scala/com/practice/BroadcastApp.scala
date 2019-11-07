package com.practice

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastApp {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("BroadcastApp")
    val sc=new SparkContext(sparkConf)
    commonJoin(sc)
    sc.stop()
  }

  def commonJoin(sc:SparkContext): Unit ={
    val g5 = sc.parallelize(Array(("501","doudou"),("502","niuge"),("527","xiaoyao"))).map(x=>(x._1,x))
    val f11 = sc.parallelize(Array(("527","xiaoyao",20))).map(x=>(x._1,x))
    g5.join(f11).map(x=>{
      x._1+","+x._2._1._2+","+x._2._2._2
    }).collect().foreach(println)

  }

}
