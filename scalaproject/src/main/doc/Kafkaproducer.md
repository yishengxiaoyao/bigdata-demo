#Kafka 生产者
Kafka的版本为2.0.0、jdk 1.8.0_231、scala 2.11.8。
##生产者客户端开发
### 必要的参数配置
>* bootstrap.servers:指定生产者客户端连接Kafka集群所需的broker地址，多个使用地址使用逗号分割。
>* key.serializer和value.serializer:broker端接收的消息必须以字节数组的形式存在。

在编写代码的过程中，参数的名称，可以使用ProducerConfig中的变量(在java代码中)，如果是scala代码，需要自己编写参数。

KafkaProducer是线程安全的，可以在多个线程中共享单个KafkaProducer实例，也可以将KafkaProducer实例进行池化来供其他线程调用。

### 消息的发送
Kafka生产者发送消息的模式为:发后即忘和异步(Kafka0.8.2之后,全部都是使用异步调用)。
#### 发后即忘
只管发送消息，不管消息是否正确到达。会出现消息丢失，这种发送方式，性能最高，可靠性最差。

#### 异步调用
Kafka发送消息都是异步，都会发送一个Future<RecordMetadata>对象，如果想要使用同步的方式，可以在后面调用get方法，
get方法会阻塞Kafka的响应,直到消息发送成功，或者发送异常。RecordMetaData中含有一些元数据:主题、分区号、分区中的偏移量、hash值。
```
try{
    Future<RecordMetadata> future = producer.send(record);
    RecordMetadata metadata = future.get();
    System.out.println(metadata.topic()+"-"+metadata.partition()+"-"+metadata.offset());
}catch (ExecutionException | InterruptedException e){ //jdk版本要合适,本人使用的jdk1.8,否则会报错。
    e.printStackTrace();
}
```
Kafka中一般发生两种类型的异常：可重试异常和不可重试异常。常见的可重试异常:NetworkException(网络故障)、LeaderNotAvailableException(分区leader副本不可用)、
UnknownTopicOrPartitionException、NotEnoughReplicasExceotion、NotCoordinatorException等。不可重试异常:RecordTooLargeException等。为了保证
消息可以顺利达到，可以设置重试次数,参数为retries。

在调用send方法来发送消息时，可以指定或者不指定调用函数，如果想用阻塞请求，可以调用get方法。
```
ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("topic-demo", "key".getBytes(), "value".getBytes());
producer.send(record,
    new Callback() {
    public void onCompletion(RecordMetadata metadata, Exception e) {
    if(e != null)
        e.printStackTrace();
    System.out.println("The offset of the record we just sent is: " + metadata.offset());
    }
});
```
同一分区发送的数据，每条记录的callback发送都是按照顺序执行的。
KafkaProducer的close方法会阻塞等待之前所有发送的请求完成之后再关闭KafkaProducer。
KafkaProducer使用total.memory.bytes来控制Producer缓存数据的最大字节数(要保证内存充足)。
异步调用，需要设置block.on.buffer.full=false。
### 序列化
>* 1.默认序列化实现
KafkaProducer默认的编码格式为UTF-8。在编写自动代码之前，需要添加相应的依赖:
```pom
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <version>1.18.10</version>
    <scope>provided</scope>
</dependency>
```
下面是自定义的序列化代码:
```java
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CompanySerializer implements Serializer<Company> {
    private String encoding = "UTF8";
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("serializer.encoding");
        if (encodingValue != null && encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    public byte[] serialize(String topic, Company data) {
        try {
            if (null == data){
                return null;
            }
            byte[] name,address;
            if (null != data.getName()){
                name = data.getName().getBytes(encoding);
            }else {
                name = new byte[0];
            }
            if (null != data.getAddress()){
                address = data.getAddress().getBytes(encoding);
            }else {
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4+4+name.length+address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        }catch (UnsupportedEncodingException e){
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + encoding);
        }
        return new byte[0];
    }
    public void close() {

    }
}
```
反序列化:
```java
package com.edu.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CompanyDeserializer implements Deserializer<Company> {

    private String encoding = "UTF8";

    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("deserializer.encoding");
        if (encodingValue != null && encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    public Company deserialize(String topic, byte[] data) {
        if (null == data){
            return null;
        }
        if (data.length < 8){
            throw new SerializationException("Size of data received by deserializer is shorter than expected!");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen,addressLen;
        String name,address;
        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);
        try {
            name = new String(nameBytes,encoding);
            address = new String(addressBytes,encoding);
        }catch (UnsupportedEncodingException e){
            throw new SerializationException("Error occur when deserializing!");
        }
        return new Company(name,address);
    }
    public void close() {

    }
}
```
>* 2.使用Protostuff实现序列化

添加相应依赖：
```
<dependency>
    <groupId>io.protostuff</groupId>
    <artifactId>protostuff-core</artifactId>
    <version>1.6.2</version>
</dependency>
<dependency>
    <groupId>io.protostuff</groupId>
    <artifactId>protostuff-runtime</artifactId>
    <version>1.6.2</version>
</dependency>
```
序列化:
```java
package com.edu.kafka;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CompanySerializer implements Serializer<Company> {
    private String encoding = "UTF8";

    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("serializer.encoding");
        if (encodingValue != null && encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    public byte[] serialize(String topic, Company data) {
        if (null == data){
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(data.getClass());
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] protostuff = null;
        try {
            protostuff = ProtobufIOUtil.toByteArray(data,schema,buffer);
        }catch (Exception e){
            throw new IllegalStateException(e.getMessage(),e);
        }finally {
            buffer.clear();
        }
        return protostuff;
    }

    public void close() {

    }
}
```
反序列化:
```java
package com.edu.kafka;

import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CompanyDeserializer implements Deserializer<Company> {

    private String encoding = "UTF8";

    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("deserializer.encoding");
        if (encodingValue != null && encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    public Company deserialize(String topic, byte[] data) {
        if (null == data){
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(Company.class);
        Company company = new Company();
        ProtostuffIOUtil.mergeFrom(data,company,schema);
        return company;
    }

    public void close() {

    }
}
```
使用序列化:
```java
package com.edu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerAnalysis {

    public static final String topic = "topic-demo";

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        KafkaProducer<String, Company> producer = new KafkaProducer<String, Company>(prop);
        Company company = Company.builder().name("xiaoyao").address("Beijing").build();
        ProducerRecord<String, Company> record = new ProducerRecord<String, Company>("topic-demo", company);
        System.out.println("===start===");
        try {
            producer.send(record);
        } catch (Exception e) {
            System.out.println("===exception===");
            e.printStackTrace();
        }

        producer.close();
        System.out.println("===finish===");
    }
}
```
### 区分器
消息经过序列化之后就需要确定他们发往的分区。如果ProducerRecord指定分区，就会发往指定的分区，
如果没有指定，就需要依赖分区器，根据key字段的哈希值选择一个分区，如果分区和key都没有指定，使用轮训的方式。
在Kafka0.8.2.2中，有一个默认的区分器:Partitioner。
在Kafka2.0.0中有一个默认分区器:DefaultPartitioner，并且提供了Partitioner接口，用户可以自定义实现分区器。

自定义分区器:
```java
package com.edu.kafka;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfos.size();
        if (null == keyBytes){
            return counter.getAndIncrement() % numPartitions;
        }else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
```
在Kafka 0.8.2.2版本中，也可以使用上面的代码，需要将implements Partitioner去掉，然后修改下面的key移动到相应分区的代码。

### 生产者拦截器
拦截器是在Kafka0.10.0.0之后才引入的功能。
KafkaProducer在将消息序列化和计算分区之前会调用生产者拦截器的onSend()方法来对消息进行相应的定制化操作。
KafkaProducer会在消息被应答之前或者消息发送失败时调用生产者拦截器的onAcknowledgement()方法，优先于
用户设定的Callback之前执行。
close()方法主要用于在关闭拦截器时执行一些资源的清理工作。
自定义的拦截器:
```java
package com.edu.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CustomProducerInterceptor implements ProducerInterceptor<String,String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix1-"+record.value();
        return new ProducerRecord<String, String>(record.topic(),record.partition(),
                record.timestamp(),record.key(),modifiedValue,record.headers());
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (null == exception){
            sendFailure++;
        }else{
            sendFailure++;
        }
    }

    public void close() {
        double successRatio = (double)sendSuccess/(sendSuccess + sendFailure);
        System.out.println("[INFO] send ratio="+String.format("%f",successRatio*100)+"%");
    }

    public void configure(Map<String, ?> configs) {

    }
}
```
KafkaProducer可以指定多个拦截器，形成拦截链，按照指定的顺序执行。
## 原理分析
### 整体架构

执行流程:
1.在启动main方法之后，KafkaProducer发送的数据到ProducerInterceptor拦截器。
2.ProducerInterceptor拦截器对数据进行过滤,然后将数据发送到序列化。
3.将过滤之后的数据发送到序列化，然后发送到不同的分区。
4.分区中数据，会发送到RecordAccumulator(消息累加器),RecordAccumulator主要用来缓存消息以便Sender线程
可以批量发送，进而减少网络的资源消耗以提升性能，默认大小为 buffer.memory=32MB,max.block.ms = 60000(超时时间)。
RecordAccumulator维护多个双端队列，数据会堆积到一个ProducerBatch(多个ProducerRecord)，然后批量发送。
消息在网络上都是以字节的形式传输，RecordAccumulator的内部有一个BufferPool,用来实现ByteBuffer的复用，以实现
缓存的高效利用。
5.Sender从RecordAccumulator中获取缓存数据，转换为<Node,List<ProducerBatch>>，Node表示broker节点。
6.将<Node,List<ProducerBatch>>转换为<NodeRequest>的形式。
7.将信息缓存起来，保存到InFlightRequests中，InFlightRequest保存对象的具体形式为Map<NodeId,Deque<Request>>,
它的主要作用是缓存已经发出去但还没有收到响应的请求。
8.将信息提交给selector准备发送。
9.selector将信息发送到KafkaCluster，KafkaCluster信息接收到信息之后，返回数据。
10.selector将KafkaCluster发送过来的数据，传递给InFlightRequests。
11.InFlightRequests接收到信息之后，会将信息发挥给主进程。

### 元数据的更新
InFlightRequests中可以获取LeastLoadedNode,即所有Node中负载最小。元数据操作是在客户端内部进行的。更新元数据时，
会挑选出LeastLoadedNode,然后这个Node发送MetadataRequest请求来获取具体的元数据信息。这里的数据同步是通过
synchronized和final来保证。

### 生产者参数
|参数|默认值|描述|
|----|----|----|
|acks|1|指定分区中必须要有多少个副本收到这条信息<br/> acks = 1,如果失败，会重发,只要有一个接收，就是成功;acks = 0,不需要等待服务端的响应；acks = -1，需要将所有的副本都返回成功才可以。|
|max.request.sizes|1MB|发送消息的最大值|
|retries|0|重试次数|
|retries.backoff.ms|100|重试之间时间间隔|
|compress.type|none|可以对消息进行压缩|
|connections.max.idle.ms|540000ms|闲置多长时间之后关闭连接|
|receive.buffer.bytes|32KB|接收消息缓存区大小|
|send.buffer.bytes|128KB|发送消息缓存区大小|
|request.timeout.ms|3000ms|等待请求响应的最大时间|
其他的参数请参考[官网](http://kafka.apache.org/20/documentation.html)




## 参考文献
[kafka消息发送模式](https://blog.csdn.net/qq_39548286/article/details/86629047)
[深入理解Kafka：核心设计与实践原理](https://book.douban.com/subject/30437872/)
[Kafka Documentation](http://kafka.apache.org/20/documentation.html)