package com.edu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("BroadcastApp")
    val sc = new SparkContext(sparkConf)
    //commonJoin(sc)
    broadcastJoin(sc)
    sc.stop()
  }

  def broadcastJoin(sc: SparkContext): Unit = {
    //driver发起，以map回来，
    val data = sc.parallelize(Array(("1", "zhangsan"), ("2", "lisi"), ("27", "xiaoyao"))).collectAsMap()
    val databroadcast = sc.broadcast(data)
    //broadcast是kv形式
    val fll = sc.parallelize(Array(("27", "beijing", "30"), ("10", "nanjing", "28")))
      .map(x => (x._1, x))
    val count = sc.longAccumulator("")

    /**
      * yeild这个关键字总结：
      * 1.for循环的每次迭代，yield产生的每一个值，都会记录在循环中。
      * 2.循环结束之后，会返回所有yield数据的集合。
      * 3.返回的集合类型与迭代的类型相同。
      */
    fll.mapPartitions(partition => {
      val datastudent = databroadcast.value //获取广播里面的内容
      for ((key, value) <- partition if (datastudent.contains(key)))
      //yield的主要作用是记住每次迭代中的有关值，并逐一存入到一个数组中
      //多个阶段需要相同数据或者以反序列化形式存储
        yield (key, datastudent.get(key).getOrElse(""), value._2)
    }).collect().foreach(println)
  }

  def commonJoin(sc: SparkContext): Unit = {
    val data = sc.parallelize(Array(("1", "zhangsan"), ("2", "lisi"), ("27", "xiaoyao"))).map(x => (x._1, x))
    val fll = sc.parallelize(Array(("27", "beijing", "30"), ("10", "nanjing", "28"))).map(x => (x._1, x))
    //后面的数字就会第几个元素,在进行join操作，需要将两个数据集都变成KV的形式。
    data.join(fll).map(x => {
      println(x._1 + ":" + x._2._1._2 + ":" + x._2._2._2)
    }).collect().foreach(println)
  }


}