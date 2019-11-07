package com.edu.spark.stream

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.config.DBs

object MySQLOffsetApp {

  def main(args: Array[String]) {
    // 准备工作
    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectKafkaApp")

    /**
      * 加载数据库配置信息
      */
    DBs.setup()
    val fromOffsets = DB.readOnly { implicit session => {
      sql"select * from offsets_storage".map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partition")), rs.long("offset"))
      }).list().apply()
    }
    }.toMap

    val topic = ValueUtils.getStringValue("kafka.topics")
    val interval = 10
    //val kafkaParams = Map[String, String]("metadata.broker.list"->"localhost:9092","auto.offset.reset"->"smallest")

    val kafkaParams = Map(
      "metadata.broker.list" -> ValueUtils.getStringValue("metadata.broker.list"),
      "auto.offset.reset" -> ValueUtils.getStringValue("auto.offset.reset"),
      "group.id" -> ValueUtils.getStringValue("group.id")
    )
    val topics = topic.split(",").toSet
    val ssc = new StreamingContext(conf, Seconds(10))
    //TODO... 去MySQL里面获取到topic对应的partition的offset
    val messages = if (fromOffsets.size == 0) { // 从头消费
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    } else { // 从指定的位置开始消费
      //val fromOffsets = Map[TopicAndPartition, Long]()
      val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("---the count of the data is ：---" + rdd.count())

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(x => {
          println(s"--${x.topic}---${x.partition}---${x.fromOffset}---${x.untilOffset}---")

          DB.autoCommit {
            implicit session =>
              sql"replace into offsets_storage(topic,groupid,partition,offset) values(?,?,?,?)"
                .bind(x.topic, ValueUtils.getStringValue("group.id"), x.partition, x.untilOffset)
                .update().apply()
          }
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
  * create table if not exists offset_storage(
  * tpoic varchar(50),
  * groupid varchar(20),
  * partition int,
  * offset bigint,
  * primary key (topic,groupid,partition)
  */
