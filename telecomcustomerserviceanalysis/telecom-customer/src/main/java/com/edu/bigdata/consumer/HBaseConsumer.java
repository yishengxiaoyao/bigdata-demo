package com.edu.bigdata.consumer;

import com.edu.bigdata.consumer.repository.HBaseDao;
import com.edu.bigdata.consumer.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;

public class HBaseConsumer {

    public static void main(String[] args) {

        // 编写 kafka 消费者，读取 kafka 集群中缓存的消息，并打印到控制台以观察是否成功

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(PropertiesUtil.properties);
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));

        HBaseDao hBaseDao = new HBaseDao();

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                // 测试
                System.out.println(record.value());

                // 将从 Kafka 中读取出来的数据写入到 HBase
                String oriValue = record.value();
                hBaseDao.put(oriValue);
            }
        }
    }
}