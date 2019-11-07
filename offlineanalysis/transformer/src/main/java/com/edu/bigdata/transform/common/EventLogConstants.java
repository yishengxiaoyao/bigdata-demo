package com.edu.bigdata.transform.common;

public class EventLogConstants {
    public static final String LOG_SEPARTIOR = "^A";
    public static final String LOG_COLUMN_NAME_IP = "ip";
    public static final String LOG_COLUMN_NAME_SERVER_TIME = "server_time";
    public static final String LOG_COLUMN_NAME_PLATFORM = "pl";
    public static final String LOG_COLUMN_NAME_COUNTRY = "country";
    public static final String LOG_COLUMN_NAME_PROVINCE = "province";
    public static final String LOG_COLUMN_NAME_CITY = "city";
    public static final String LOG_COLUMN_NAME_USER_AGENT = "b_iev";
    public static final String LOG_COLUMN_NAME_BROWSER_NAME = "b_name";
    public static final String LOG_COLUMN_NAME_BROWSER_VERSION = "b_ver";
    public static final String LOG_COLUMN_NAME_OS_NAME = "os_name";
    public static final String LOG_COLUMN_NAME_OS_VERSION = "os_version";

    public static final String LOG_COLUMN_NAME_EVENT_NAME = "en";
    public static final String LOG_COLUMN_NAME_EVENT_CATEGORY = "ca";
    public static final String LOG_COLUMN_NAME_EVENT_ACTION = "ac";
    public static final String LOG_COLUMN_NAME_CURRENT_URL = "p_url";
    public static final String LOG_COLUMN_NAME_VERSION = "ver";

    public static final String LOG_COLUMN_NAME_MEMBER_ID = "u_mid";
    public static final String EVENT_LOGS_FAMILY_NAME = "";

    public static final String LOG_COLUMN_NAME_UUID = "u_ud";
    public static final String HBASE_NAME_EVENT_LOGS = "event_logs";
    public static final String LOG_COLUMN_NAME_ORDER_ID = "oid";
    public static final String LOG_COLUMN_NAME_ORDER_CURRENCY_TYPE = "cut";
    public static final String LOG_COLUMN_NAME_ORDER_PAYMENT_TYPE = "pt";
    public static final String LOG_COLUMN_NAME_ORDER_CURRENCY_AMOUNT = "cua";

    public static final byte[] BYTES_EVENT_LOGS_FAMILY_NAME = "".getBytes();

    public enum EventEnum {
        CHARGEREFUND("e_cr"),
        CHARGESUCCESS("e_cs"),
        CHARGEREQUEST("e_crt"),
        LAUNCH("e_l"),
        EVENT("e_e"),
        PAGEVIEW("e_pv");

        public String alias;

        EventEnum(String alias) {
            this.alias = alias;
        }

        public static EventEnum valueOfAlias(String eventName) {
            switch (eventName) {
                case "e_cr":
                    return CHARGEREFUND;
                case "e_cs":
                    return CHARGESUCCESS;
                case "e_crt":
                    return CHARGEREQUEST;
                case "e_e":
                    return EVENT;
                case "e_l":
                    return LAUNCH;
                case "e_pv":
                    return PAGEVIEW;
                default:
                    return null;
            }
        }
    }

    public static class PlatformNameConstants {
        public static final String JAVA_SERVER_SDK = "java_server";
        public static final String PC_WEBSITE_SDK = "website";
    }


}
