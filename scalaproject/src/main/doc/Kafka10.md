# Spark Streaming + Kafka
Kafka分区和Spark分区以及访问offset和metadata都是1:1比例。

## 添加依赖
```
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-streaming-kafka-0-10_2.11</artifactId>
  <version>${spark.version}</version>
</dependency>
```
只需要添加这个依赖，其他的不需要。

## 创建Direct Stream


## LocationStrategies

消费API会预先获取消息将数据放到缓冲区，在executor上保留缓存consumer，在主机上根据consumer设置合适的分区。

在大多数情况下，使用LocationStrategies.PreferConsistent，这个方式可以将数据均匀分布在各个executors。
如果executors和Kafka broker在同一主机上，使用LocationStrategies.PreferBrokers，可以在Kafka leader调度分区。
如果数据倾斜，使用LocationStrategies.PreferFixed，这个方式可以明确分区于主机的映射关系。

consumer默认缓存大小为64。如果想要处理超过kafka分区(64*executors数量)，可以修改spark.streaming.kafka.consumer.cache.maxCapacity。

取消consumer缓存，使用spark.streaming.kafka.consumer.cache.enabled=false。

在缓存数据时，使用topicpartition和group.id作为key，可以根据不同的group.id来调用createDirectStream。

## ConsumerStrategies

新的consumer api有很多种方法来指定topic，ConsumerStrategies提供了一种抽象，保证在检查点重启之后，Spark可以获取正确订阅的消费者。

ConsumerStrategies.Subscribe允许订阅哪些固定的topic。ConsumerStrategies.SubscribePattern可以通过正则表达式来订阅自己喜欢的topic。
于0.8版本不同的是，在执行的stream中，使用Subscribe或者SubscribePattern，可以返回添加的分区。ConsumerStrategies.Assign允许自定固定的分区集合。

如果自定义消费策略，需要继承ConsumerStrategies。