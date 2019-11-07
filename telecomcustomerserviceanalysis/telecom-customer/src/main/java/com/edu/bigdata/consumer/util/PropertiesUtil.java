package com.edu.bigdata.consumer.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {

    public static Properties properties = null;

    static {
        // 加载配置文件的属性
        InputStream is = ClassLoader.getSystemResourceAsStream("kafka.properties");
        properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}