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
