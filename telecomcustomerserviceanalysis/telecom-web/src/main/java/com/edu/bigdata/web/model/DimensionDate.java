package com.edu.bigdata.web.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "tb_dimension_date")
public class DimensionDate implements Serializable {
    @Id
    @Column(name = "id")
    private int id;
    @Column(name = "year")
    private int year;
    @Column(name = "month")
    private int month;
    @Column(name = "day")
    private int day;

    public DimensionDate() {
    }

    public DimensionDate(int id, int year, int month, int day) {
        this.id = id;
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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
}
