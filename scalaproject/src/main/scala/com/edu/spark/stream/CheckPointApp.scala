package com.edu.spark.stream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object CheckPointApp {
  def main(args: Array[String]): Unit = {
    offsetManage()
  }

  def offsetManage(): Unit = {
    // 准备工作
    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectKafkaApp")
    /**
      * 修改代码之后不能用(checkpoint)。
      * 小文件比较多
      */
    val checkpointPath = "hdfs://hadoop000:8020/offset_xiaoyao/checkpoint"
    val topic = "test"
    val interval = 10
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "hadoop000:9092", "auto.offset.reset" -> "smallest")
    val topics = topic.split(",").toSet

    def function2CreateStreamingContext(): StreamingContext = {
      val ssc = new StreamingContext(conf, Seconds(10))
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      ssc.checkpoint(checkpointPath)
      messages.checkpoint(Duration(8 * 10.toInt * 1000))

      messages.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          println("---count of the data is ：---" + rdd.count())
        }
      })
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointPath, function2CreateStreamingContext)
    ssc.start()
    ssc.awaitTermination()
  }
}
