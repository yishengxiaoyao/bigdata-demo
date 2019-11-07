package com.edu.bigdata.session.kafka

import java.util.Properties

abstract class KafkaProducerFactory(brokerList: String,
                                    config: Properties,
                                    topic: Option[String] = None)
  extends Serializable {
  def newInstance(): KafkaProducerProxy
}
