package com.edu.bigdata.session.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

case class KafkaProducerProxy(brokerList: String,
                              producerConfig: Properties = new Properties,
                              defaultTopic: Option[String] = None,
                              producer: Option[KafkaProducer[String, String]] = None) {

  type Key = String
  type Val = String

  require(brokerList == null || !brokerList.isEmpty, "Must set broker list")

  private val p = producer getOrElse {

    var props:Properties= new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    new KafkaProducer[String,String](props)
  }


  //把我的消息包装成了ProducerRecord
  private def toMessage(value: Val, key: Option[Key] = None, topic: Option[String] = None): ProducerRecord[Key, Val] = {
    val t = topic.getOrElse(defaultTopic.getOrElse(throw new IllegalArgumentException("Must provide topic or default topic")))
    require(!t.isEmpty, "Topic must not be empty")
    key match {
      case Some(k) => new ProducerRecord(t, k, value)
      case _ => new ProducerRecord(t, value)
    }
  }

  def send(key: Key, value: Val, topic: Option[String] = None) {
    //调用KafkaProducer他的send方法发送消息
    p.send(toMessage(value, Option(key), topic))
  }

  def send(value: Val, topic: Option[String]) {
    send(null, value, topic)
  }

  def send(value: Val, topic: String) {
    send(null, value, Option(topic))
  }

  def send(value: Val) {
    send(null, value, None)
  }

  def shutdown(): Unit = p.close()

}