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
对于消费者使用集合的方式来订阅主题而言，如果订阅了不同的主题，消费者k以最后一次的为准。
如果使用的正则表达式，新创建的主题，也可以匹配。上面两种都是使用的subscribe()方法。
也可以指定订阅某个分区的数据，使用assign(Collection<TopicPartition> partition)方法。
使用unsubscribe方法来取消对主题的订阅。

### 消费消息
Kafka中的消费是基于拉模式。推模式是服务端将消息主动推送给消费者，拉模式是消费者主动向服务端发起请求来拉取消息。
消费者是需要不断轮训，拉取消息。有的时候，需要按照主题进行消费。
如果条件允许，建议使用poll(Duration)的方式拉取数据。

```
ConsumerRecords<String,String> records = consumer.poll(1000);
for (TopicPartition tp:records.partitions()){
    for (ConsumerRecord record:records.records(topic)){
        System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
    }
}
```
### 位移提交
在旧消费者客户端中，消费位移是存储在Zookeeper中。在新消费者客户端中，消费者位移存储在Kafka内部的主题__consumer_offsets中。
这里把消费位移存储起来的动作成为提交，消费者在消费完消息之后需要执行消费者位移的提交。
Kafka中默认的消费位移的提交方式是自动提交，参数为enable.auto.commit,默认值为true。
定期提交的时间间隔为auto.commit.interval.ms=5s。

### 控制或关闭消费

KafkaConsumer中使用pause()和resume()方法来分别实现暂停某些分区在拉取操作时返回数据给客户端和恢复某些分区向客户端返回数据的操作。
wakeup()方法是KafkaConsumer中唯一可以从其他线程里安全调用的方法。

### 指定位移消费
消费者客户端参数:auto.offset.rest的配置来决定从何处开始进行消费，默认指为latest，表示从分区末尾开始消费消息。
seek()方法只能重置消费者分配到的分区的消费位置，而分区的分配是在poll()方法的调用过程中实现的。
在执行seek()方法之前需要先执行一次poll()方法，等到分配到分区之后才可以重置消费位置。
可以从分区开始(seekToBeginning())、结束(seekToEnd())、指定时间(offsetsForTimes)读取数据。
为了保证KafkaConsume可以正常消费到数据，将消费的offset存储在数据库中，每次读取数据时，可以数据库中读取最新的offset。


### 再均衡
再均衡是指分区的所属权从一个消费者转移到另一个消费者的行为，它为消费组具备高可用性和伸缩性提供保证，这样就可以
既方便又安全地删除消费组内的消费者或往消费组内添加消费者。再均衡发生期间，消费组内的消费者无法读取消息。尽量避免再均衡的发生。
再均衡监听器和数据库配合使用。

### 消费者拦截器
```java
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerInterceptorTTL implements ConsumerInterceptor<String,String> {
    private static final long EXPIRE_INTERVAL = 10 * 1000;
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String,String>>> newRecords = new HashMap<>();
        for (TopicPartition tp:consumerRecords.partitions()){
            List<ConsumerRecord<String,String>> tpRecords = consumerRecords.records(tp);
            List<ConsumerRecord<String,String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String,String> record:tpRecords){
                if (now - record.timestamp() < EXPIRE_INTERVAL){
                    newTpRecords.add(record);
                }
            }
            if (!newRecords.isEmpty()){
                newRecords.put(tp,newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        map.forEach((tp,offset)->{
            System.out.println(tp+":"+offset.offset());
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
```

