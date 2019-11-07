package com.edu.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object UpdateByKeyApp {
  def main(args: Array[String]): Unit = {
    socketStream()
  }

  def socketStream(): Unit = {
    //做一个开关
    //将需要过滤的数据写到数据库中
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    //如果是有状态的操作，需要要设置checkpint
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("localhost", 9999)
    val result = lines.flatMap(_.split(",")).map(x => (x, 1))
    val state = result.updateStateByKey(updateFunction)
    state.print()
    //开始streaming
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val curr = currentValues.sum
    val prev = preValues.getOrElse(0)
    Some(curr + prev)
  }

}
