package com.edu.bigdata.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.Reader;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;


/**
 * 地理工具类，实现通过 ip 查找地址区域
 */
public class GeoUtil {

    private static Reader reader;

    private static ResourceLoader resourceLoader = new DefaultResourceLoader();;

    /**
     * 获得国家数据
     */
    public static String getCountry(String ip) {
        try {
            InputStream inputStream=resourceLoader.getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");
            //Resource resource = new ClassPathResource("GeoLite2-City.mmdb");
            //reader = new Reader(resource.getFile());
            reader = new Reader(inputStream);


            if (reader != null) {
                JsonNode node = reader.get(InetAddress.getByName(ip));

                if (node != null) {
                    JsonNode countryNode = node.get("country");

                    if (countryNode != null) {
                        JsonNode namesNode = countryNode.get("names");

                        if (namesNode != null) {
                            JsonNode zhNode = namesNode.get("zh-CN");

                            if (zhNode != null) {
                                return zhNode.textValue();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return "";
    }

    /**
     * 获得省份数据
     */
    public static String getProvince(String ip) {

        try {

            //Resource resource = new ClassPathResource("GeoLite2-City.mmdb");
            //reader = new Reader(resource.getFile());
            InputStream inputStream=resourceLoader.getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");
            reader = new Reader(inputStream);
            if (reader != null) {
                JsonNode node = reader.get(InetAddress.getByName(ip));

                if (node != null) {
                    JsonNode subdivisionsNode = node.get("subdivisions");
                    if (subdivisionsNode != null) {
                        JsonNode areaNode = subdivisionsNode.get(0);

                        if (areaNode != null) {
                            JsonNode namesNode = areaNode.get("names");

                            if (namesNode != null) {
                                JsonNode zhNode = namesNode.get("zh-CN");

                                if (zhNode != null) {
                                    return zhNode.textValue();
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return "";
    }
}