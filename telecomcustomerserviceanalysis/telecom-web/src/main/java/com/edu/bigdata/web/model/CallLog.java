package com.edu.bigdata.web.model;


import java.io.Serializable;

/**
 * 封装从Mysql中取出来的数据
 */
public class CallLog implements Serializable {
    private String id_contact_date;
    private int id_dimension_contact;
    private int id_dimension_date;
    private int call_sum;
    private int call_duration_sum;
    private String telephone;
    private String name;
    private int year;
    private int month;
    private int day;

    public String getId_contact_date() {
        return id_contact_date;
    }

    public void setId_contact_date(String id_contact_date) {
        this.id_contact_date = id_contact_date;
    }

    public int getId_dimension_contact() {
        return id_dimension_contact;
    }

    public void setId_dimension_contact(int id_dimension_contact) {
        this.id_dimension_contact = id_dimension_contact;
    }

    public int getId_dimension_date() {
        return id_dimension_date;
    }

    public void setId_dimension_date(int id_dimension_date) {
        this.id_dimension_date = id_dimension_date;
    }

    public int getCall_sum() {
        return call_sum;
    }

    public void setCall_sum(int call_sum) {
        this.call_sum = call_sum;
    }

    public int getCall_duration_sum() {
        return call_duration_sum;
    }

    public void setCall_duration_sum(int call_duration_sum) {
        this.call_duration_sum = call_duration_sum;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    @Override
    public String toString() {
        return "CallLog{" +
                "id_contact_date='" + id_contact_date + '\'' +
                ", id_dimension_contact=" + id_dimension_contact +
                ", id_dimension_date=" + id_dimension_date +
                ", call_sum=" + call_sum +
                ", call_duration_sum=" + call_duration_sum +
                ", telephone='" + telephone + '\'' +
                ", name='" + name + '\'' +
                ", year=" + year +
                ", month=" + month +
                ", day=" + day +
                '}';
    }
}
