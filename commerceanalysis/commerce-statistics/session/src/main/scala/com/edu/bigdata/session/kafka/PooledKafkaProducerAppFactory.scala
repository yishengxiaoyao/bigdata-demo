package com.edu.bigdata.session.kafka

import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.DefaultPooledObject

// 继承一个基础的连接池，需要提供池化的对象类型
class PooledKafkaProducerAppFactory(val factory: KafkaProducerFactory)
  extends BasePooledObjectFactory[KafkaProducerProxy] with Serializable {

  // 用于池来创建对象
  override def create(): KafkaProducerProxy = factory.newInstance()

  // 用于池来包装对象
  override def wrap(obj: KafkaProducerProxy): PooledObject[KafkaProducerProxy] = new DefaultPooledObject(obj)

  // 用于池来销毁对象
  override def destroyObject(p: PooledObject[KafkaProducerProxy]): Unit = {
    p.getObject.shutdown()
    super.destroyObject(p)
  }

}
