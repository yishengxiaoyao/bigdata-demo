package com.edu.bigdata.transformer;

import com.edu.bigdata.transform.util.TimeUtil;
import com.edu.bigdata.transform.util.UserAgentUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

public class DateTimeTest {


    public static void main(String[] args){
       /* Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, -1);
        FastDateFormat df2 = FastDateFormat.getInstance("yyyy-MM-dd");
        System.out.println(df2.format(cal.getTime()));*/
        /*try {
            CloseableHttpClient client = HttpClients.createDefault();
            HttpGet get = new HttpGet("http://ip.taobao.com/service/getIpInfo.php?ip=119.75.217.109");
            CloseableHttpResponse response = client.execute(get);
            HttpEntity entity = response.getEntity();
            System.out.println(EntityUtils.toString(entity));
        }catch (ClientProtocolException e){

        }catch (IOException e){

        }*/
        /*String data = "{\"code\":0,\"data\":{\"ip\":\"119.75.217.109\",\"country\":\"中国\",\"area\":\"\",\"region\":\"北京\",\"city\":\"北京\",\"county\":\"XX\",\"isp\":\"电信\",\"country_id\":\"CN\",\"area_id\":\"\",\"region_id\":\"110000\",\"city_id\":\"110100\",\"county_id\":\"xx\",\"isp_id\":\"100017\"}}";
        Gson gson=new Gson();
        JsonObject json = gson.fromJson(data, JsonObject.class);
        if (json.get("code").getAsInt()==0){
            JsonObject other = json.getAsJsonObject("data");
            String country = other.get("country").getAsString();
            String province = other.get("region").getAsString();
            String city = other.get("city").getAsString();
        }else{

        }*/
        /*String other="u_nu=1&u_sd=6D4F89C0-E17B-45D0-BFE0-059644C1878D&c_time=1450569596991&ver=1&en=e_l&pl=website&sdk=js&b_rst=1440*900&u_ud=4B16B8BB-D6AA-4118-87F8-C58680D22657&b_iev=Mozilla%2F5.0%20(Windows%20NT%205.1)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F45.0.2454.101%20Safari%2F537.36&l=zh-CN&bf_sid=33cbf257-3b11-4abd-ac70-c5fc47afb797_11177014";
        UserAgentInfo userAgentInfo = UserAgentUtil.analyticUserAgent(other);
        System.out.println("browser version");
        System.out.println(userAgentInfo.getBrowserVersionInfo());
        System.out.println("浏览器名称");
        System.out.println(userAgentInfo.getUaFamily());
        System.out.println("操作系统名称");
        System.out.println(userAgentInfo.getOsFamily());
        System.out.println("操作系统版本");
        System.out.println(userAgentInfo.getOsName());*/
        /*String time = "1555318954.798";
        System.out.println(TimeUtil.parseNginxServerTime2Long(time));*/
        DateTime now = DateTime.now();
        System.out.println(now.withDayOfMonth(1).plusMonths(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0));

        for (int i=1;i<13;i++) {
            int season = Double.valueOf(Math.floor((i+2) / 3)).intValue();
            System.out.println(season);
        }
    }
}
