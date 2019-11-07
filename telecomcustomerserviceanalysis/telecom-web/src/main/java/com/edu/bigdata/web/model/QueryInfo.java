package com.edu.bigdata.web.model;

import java.io.Serializable;

/**
 * 封装用于用户传递过来的数据
 */
public class QueryInfo implements Serializable {

    private String telephone;

    private String year;

    private String month;

    private String day;

    public QueryInfo() {
    }

    public QueryInfo(String telephone, String year, String month, String day) {
        this.telephone = telephone;
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Override
    public String toString() {
        return "QueryInfo{" +
                "telephone='" + telephone + '\'' +
                ", year='" + year + '\'' +
                ", month='" + month + '\'' +
                ", day='" + day + '\'' +
                '}';
    }
}