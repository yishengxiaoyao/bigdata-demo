package com.edu.spark.stream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectKafkaApp {

  def main(args: Array[String]) {
    directKafka()
  }

  def directKafka(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectKafkaApp")
    val ssc = new StreamingContext(conf, Seconds(10))
    val topic = "test"
    /**
      * 必须要明确broker的地址：metadata.broker.list或者bootstrap.servers
      */
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val topics = topic.split(",").toSet
    val offsetRanges = Array.empty[OffsetRange]

    /**
      * 创建输入流从kafka中拉取数据，不实用receiver。保证数据只被处理一次
      * 重点：
      * 1.没有receiver。
      * 2.offset:offset不存储在zookeeper中，自己更新offset。
      * 3.故障恢复：开启checkpoint来存储offset
      * 4.端到端语义：数据只被处理一次，输出不一定是只有一次，需要使用幂等操作或者使用事务来保证只输出一次
      */
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    messages.transform(rdd => {
      var offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).foreachRDD(rdd => {
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    })
    messages.map(_._2) // 取出value
      .flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
