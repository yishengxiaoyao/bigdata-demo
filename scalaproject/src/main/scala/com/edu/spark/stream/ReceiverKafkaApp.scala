package com.edu.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiverKafkaApp {
  def main(args: Array[String]): Unit = {
    kafkaStream()
  }

  def kafkaStream(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topic = "test"
    val numberPartitions = 1
    val zkQuorm = "localhost:2181"
    val groupId = "tes"
    //分区与并行度没有关系
    val topics = topic.split(",").map((_, numberPartitions)).toMap
    val messages = KafkaUtils.createStream(ssc, zkQuorm, groupId, topics)
    messages.map(_._2) // 取出value
      .flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
}
