package com.edu.bigdata;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] dummy, byte[] line) { // dummy 表示 哑变量，没什么用
        // 把收集到的日志信息用 String 表示
        String input = new String(line);
        // 根据前缀 PRODUCT_RATING_PREFIX: 从日志信息中提取评分数据
        if (input.contains("PRODUCT_RATING_PREFIX:")) {
            System.out.println("product rating data coming! >>>>>>>>>>>>>>>>>>>> " + input);

            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(), input.getBytes());
        }
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}