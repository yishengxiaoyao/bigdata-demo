package com.edu.spark.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._

object Kafka10App {
  def main(args: Array[String]): Unit = {
    createDirectStream()
  }

  def createDirectStream(): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "xiaoyao",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topicA", "topicB")
    /*val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )*/

    val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      OffsetRange("test", 0, 0, 100),
      OffsetRange("test", 1, 0, 100)
    )

    /*val rdd = KafkaUtils.createRDD[String, String](sparkContext, kafkaParams, offsetRanges, PreferConsistent)

    stream.foreachRDD(rdd=>{
      val offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition{
        iter=>{
          val o:OffsetRange=offsetRanges(TaskContext.getPartitionId())
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }
      }
    })*/
  }
}
