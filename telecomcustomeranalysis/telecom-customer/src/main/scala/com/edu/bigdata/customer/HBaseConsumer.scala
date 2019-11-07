package com.edu.bigdata.customer

import java.util.{Arrays, Properties}

import com.edu.bigdata.customer.util.PropertiesUtil
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

object HBaseConsumer {
  def main(args: Array[String]): Unit = {
    /*val properties = new Properties()
    properties.put("bootstrap.servers",PropertiesUtil.getProperty("bootstrap.servers"))
    properties.put("group.id",PropertiesUtil.getProperty("group.id"))
    properties.put("key.deserializer", PropertiesUtil.getProperty("key.deserializer"))
    properties.put("value.deserializer", PropertiesUtil.getProperty("value.deserializer"))
    properties.put("enable.auto.commit", PropertiesUtil.getProperty("enable.auto.commit"))
    properties.put("auto.commit.interval.ms",PropertiesUtil.getProperty("auto.commit.interval.ms"))*/
    val kafkaConsumer = new KafkaConsumer[String,String](PropertiesUtil.properties)
    kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")))


  }
}
