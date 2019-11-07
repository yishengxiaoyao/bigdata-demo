package com.edu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerAnalysis {

    public static final String topic = "topic-demo";

    public static void main(String[] args) {
        /*Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic-demo", "Hello Kafka from program idea2423423");
        System.out.println("===start===");
        try {
            producer.send(record);
        } catch (Exception e) {
            System.out.println("===exception===");
            e.printStackTrace();
        }

        producer.close();
        System.out.println("===finish===");*/
        /*Properties prop = new Properties();
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
        System.out.println("===finish===");*/

    }
}
