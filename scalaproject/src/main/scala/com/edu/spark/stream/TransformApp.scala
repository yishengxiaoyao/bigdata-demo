package com.edu.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object TransformApp {
  //黑名单过滤
  def main(args: Array[String]): Unit = {
    socketStream()
  }

  def socketStream(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    val blackTuple = new ListBuffer[(String, Boolean)]
    blackTuple.append(("yishengxiaoyao", true))
    blackTuple.append(("xiaoyao", true))
    val lines = ssc.socketTextStream("localhost", 9999)
    val blackRdd = ssc.sparkContext.parallelize(blackTuple)
    lines.map(x => ((x.split(",")) (0), x)).transform(rdd => {
      rdd.leftOuterJoin(blackRdd).filter(x => {
        x._2._2.getOrElse(false) != true
      }).map(_._2._1)
    }).print()

    //开始streaming
    ssc.start()
    ssc.awaitTermination()
  }
}
