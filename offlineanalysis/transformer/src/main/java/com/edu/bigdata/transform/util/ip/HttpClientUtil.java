package com.edu.bigdata.transform.util.ip;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpClientUtil {

    public static JsonObject httpGet(String url){
        CloseableHttpClient client = HttpClients.createDefault();
        try {
            HttpGet get = new HttpGet(url);
            CloseableHttpResponse response = client.execute(get);
            HttpEntity entity = response.getEntity();
            Gson gson = new Gson();
            JsonObject json = gson.fromJson(EntityUtils.toString(entity),JsonObject.class);
            int code = json.get("code").getAsInt();
            if(code==0){
                return json.get("data").getAsJsonObject();
            }
        }catch (ClientProtocolException e){

        }catch (IOException e){

        }finally {
            if (client!=null){
                try {
                    client.close();
                }catch (IOException e){
                    e.printStackTrace();;
                }
            }
        }
        return null;
    }

}
