package com.edu.spark.stream;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * Kafka数据发送工具类
 */
public class DIYKafkaProducer {

    public static void main(String[] args) {

        String topic = "test";
        Properties properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", "localhost:9092");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);

        for (int index = 0; index < 100; index++) {
            producer.send(new KeyedMessage<String, String>(topic, index + "", index + "students：" + UUID.randomUUID()));
        }

        System.out.println("produce data start...");

    }


}
