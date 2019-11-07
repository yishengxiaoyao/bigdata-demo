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

    /*public Company deserialize(String topic, byte[] data) {
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
    }*/

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
