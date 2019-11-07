package com.edu.bigdata;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {

    public static void main(String[] args) {

        String brokers = "hadoop102:9092";
        String zookeepers = "hadoop102:2181";

        // 定义输入和输出的 topic
        String from = "log";
        String to = "ECrecommender";

        // 定义 kafka streaming 的配置
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        StreamsConfig config = new StreamsConfig(settings);

        // 拓扑建构器
        TopologyBuilder builder = new TopologyBuilder();



        // 定义流处理的拓扑结构
        builder.addSource("SOURCE", from)
                .addProcessor("PROCESSOR", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK", to, "PROCESSOR");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
        
        System.out.println("kafka stream started!");
    }
}