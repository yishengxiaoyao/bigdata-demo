package com.book.code.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue

object QueueStream {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("QueueStream")

    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new Queue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = ssc.queueStream(rddQueue)

    // 对 x % 10 的进行计数
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream.print()
    ssc.start()

    // Create and push some RDDs into
    // 注意可以启动之后再进行 push
    for (i <- 1 to 30) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(for (i <- 1 to 1000) yield scala.util.Random.nextInt(1000), 10)
      }
      Thread.sleep(1000)
    }

    // 注意这里是 stop
    ssc.stop()
  }
}