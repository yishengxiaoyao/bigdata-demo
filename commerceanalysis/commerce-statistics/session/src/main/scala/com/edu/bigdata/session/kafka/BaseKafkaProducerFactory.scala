package com.edu.bigdata.session.kafka

import java.util.Properties

class BaseKafkaProducerFactory(brokerList: String,
                               config: Properties = new Properties,
                               defaultTopic: Option[String] = None)
  extends KafkaProducerFactory(brokerList, config, defaultTopic) {

  override def newInstance() = new KafkaProducerProxy(brokerList, config, defaultTopic)

}