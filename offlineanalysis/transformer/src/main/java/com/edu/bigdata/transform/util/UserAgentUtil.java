package com.edu.bigdata.transform.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;

import java.io.IOException;

public class UserAgentUtil {

    static UASparser uaSparser = null;

    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static UserAgentInfo analyticUserAgent(String userAgentInfo) {
        try {
            if (userAgentInfo != null) {
                return uaSparser.parse(userAgentInfo);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
