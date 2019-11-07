package com.edu.spark.stream


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowApp {
  def main(args: Array[String]): Unit = {
    socketStream()
  }

  def socketStream(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)
    val results = lines.flatMap(x => x.split(","))
      .map((_, 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(10), Seconds(5)).print()
    //开始streaming
    ssc.start()
    ssc.awaitTermination()
  }

}
