package com.edu.bigdata.transform.util;


import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.joda.time.DateTime;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;


public class TimeUtil {

    public static final String HBASE_TABLE_NAME_SUFFIX_FORMAT = "yyyyMMdd";

    public static FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd");

    public static long getTodayInMillis() {
        return DateTime.now().getMillis();
    }

    /**
     * 将带有小数点的字符串变成long类型，需要先进行拆分
     *
     * @param date
     * @return
     */
    public static long parseNginxServerTime2Long(String date) {
        return Long.valueOf(date.split("\\.")[0]);
    }

    /**
     * 将时间字符串转换为时间戳
     * @param date
     * @return
     */
    public static long parseString2Long(String date) {
        try {
           return df.parse(date).getTime();
        }catch (ParseException e){
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * 根据用户设置的格式化字符串，将时间戳转换为时间字符串。
     * @param second
     * @param pattern
     * @return
     * @throws IOException
     */
    public static String parseLong2String(long second, String pattern) throws IOException {
        FastDateFormat df1 = FastDateFormat.getInstance(pattern);
        if (second == 0L) {
            throw new IOException("解析时间出现问题");
        }
        return df1.format(second);
    }

    public static String getYesterday() {
        return df.format(DateTime.now().minusDays(1));
    }

    /**
     * 判断日期是否为当前时间
     * @param date
     * @return
     */
    public static boolean isValidateRunningDate(String date) {
        try {
            Date input =  df.parse(date);
            Date tody = new Date();
            return DateUtils.isSameDay(input,tody);
        }catch (ParseException e){
            e.printStackTrace();
        }
        return false;
    }

    public static long getFirstDayOfThisWeek(long date) {
        return DateTime.parse(df.format(date)).withDayOfWeek(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).getMillis();
    }

    public static long getFirstDayOfNextWeek(long date) {
        return DateTime.parse(df.format(date)).withDayOfWeek(1).plusDays(7).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).getMillis();
    }

    public static long getFirstDayOfThisMonth(long date) {
        return DateTime.parse(df.format(date)).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).getMillis();
    }

    public static long getFirstDayOfNextMonth(long date) {
        return DateTime.parse(df.format(date)).withDayOfMonth(1).plusMonths(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).getMillis();
    }
}
