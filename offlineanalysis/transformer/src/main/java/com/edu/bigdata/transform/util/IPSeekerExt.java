package com.edu.bigdata.transform.util;

import com.edu.bigdata.transform.util.ip.HttpClientUtil;
import com.google.gson.JsonObject;

public class IPSeekerExt {

    private static IPSeekerExt ext = new IPSeekerExt();

    public static IPSeekerExt getInstance() {
        return ext;
    }

    public RegionInfo analysisIp(String ip) {
        //解析IP的国家、省市、城市。
        String url = "http://ip.taobao.com/service/getIpInfo.php?ip="+ip;
        JsonObject result = HttpClientUtil.httpGet(url);
        RegionInfo regionInfo=null;
        if (result != null){
            String country = result.get("city").getAsString();
            String province = result.get("region").getAsString();
            String city = result.get("city").getAsString();
            regionInfo = new RegionInfo(country,province,city);
        }
        return regionInfo;
    }

    public class RegionInfo {

        private String country;
        private String province;
        private String city;

        public RegionInfo() {
        }

        public RegionInfo(String country, String province, String city) {
            this.country = country;
            this.province = province;
            this.city = city;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }


}
