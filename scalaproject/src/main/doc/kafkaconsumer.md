# Kafka 消费者

## 消费者与消费组

每个分区只能被一个消费组中的一个消费者所消息。
消息的传递模式:点对点模式和发布/订阅模式。
>* 点对点模式是基于队列的，消息生产者发送消息到队列，消息消费者从队列中接收消息。
>* 发布/订阅模式:消息发送一个topic里面，消费者从响应topic中获取数据，订阅者和发布者都是独立的，
发布/订阅在消息的一对多广播时采用。

Kafka支持两种消费投递模式:
>* 如果所有的消费者都是在同一个消费组,所有的消息都会被均衡地投递给每一个消费者(类似点对点)。
>* 如果所有的消费者属于不同的消费组,消息就会广播给所有的消费者(发布/订阅模式)。

## 客户端开发
### 订阅主题与分区
对于消费者使用集合的方式来订阅主题而言，如果订阅了不同的主题，消费者以最后一次的为准。
如果使用的正则表达式，新创建的主题，也可以匹配。上面两种都是使用的subscribe()方法。
也可以指定订阅某个分区的数据，使用assign(Collection<TopicPartition> partition)方法。
使用unsubscribe方法来取消对主题的订阅。

### 消费消息
Kafka中的消费是基于拉模式。推模式是服务端将消息主动推送给消费者，拉模式是消费者主动向服务端发起请求来拉取消息。
消费者是需要不断轮训，拉取消息。有的时候，需要按照主题进行消费。
```
ConsumerRecords<String,String> records = consumer.poll(1000);
for (TopicPartition tp:records.partitions()){
    for (ConsumerRecord record:records.records(topic)){
        System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
    }
}
```
### 
