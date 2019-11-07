package com.edu.bigdata.transform.common;

public enum DateEnum {

    DAY,WEEK,MONTH,SEASON,YEAR;

    public static DateEnum valueOfName(String type) {
        switch (type) {
            case "day":
                return DAY;
            case "week":
                return WEEK;
            case "month":
                return MONTH;
            case "season":
                return SEASON;
            case "year":
                return YEAR;
            default:
                return null;
        }
    }
}
