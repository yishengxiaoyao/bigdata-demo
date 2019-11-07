package com.edu.bigdata.transform.dimension.key.base;

import com.edu.bigdata.transform.common.DateEnum;
import com.edu.bigdata.transform.dimension.base.BaseDimension;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateDimension extends BaseDimension {

    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;

    public DateDimension(int year, int season, int month, int week, int day) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
    }

    public DateDimension() {
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

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }

        if (o == null || getClass() != o.getClass()){
            return false;
        }

        DateDimension dd = (DateDimension) o;

        if (this.year != dd.getYear()) return false;
        if (this.month != dd.getMonth()) return false;
        return this.day == dd.getDay();
    }

    @Override
    public int hashCode() {
        int result = year;
        result = 31 * result + month;
        result = 31 * result + day;
        return result;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) return 0;
        DateDimension dd = (DateDimension) o;

        int result = Integer.compare(this.id, dd.getId());
        if (result != 0) return result;

        result = Integer.compare(this.year, dd.getYear());
        if (result != 0) return result;

        result = Integer.compare(this.season,dd.getSeason());
        if (result != 0) return result;

        result = Integer.compare(this.month, dd.getMonth());
        if (result != 0) return result;

        result = Integer.compare(this.week,dd.getWeek());
        if (result != 0) return result;

        result = Integer.compare(this.day, dd.getDay());
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (out != null){
            out.writeInt(this.id);
            out.writeInt(this.year);
            out.writeInt(this.season);
            out.writeInt(this.month);
            out.writeInt(this.week);
            out.writeInt(this.day);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (in != null){
            this.id = in.readInt();
            this.year = in.readInt();
            this.season = in.readInt();
            this.month = in.readInt();
            this.week = in.readInt();
            this.day = in.readInt();
        }
    }

    public static DateDimension buildDate(long serverTime,DateEnum date){
        FastDateFormat fdf = FastDateFormat.getInstance("yyyy-MM-dd");
        String time = fdf.format(serverTime);
        int year = Integer.valueOf(time.substring(0,4));
        int month = Integer.valueOf(time.substring(4,6));
        int season = Double.valueOf(Math.floor((month+2)/3)).intValue();
        int week = 0;
        int day = Integer.valueOf(time.substring(6,8));
        if (date == DateEnum.YEAR){
            return new DateDimension(year,-1,-1,-1,-1);
        }else if (date == DateEnum.SEASON){
            return new DateDimension(year,season,-1,-1,-1);
        }else if (date == DateEnum.MONTH){
            return new DateDimension(year,-1,month,-1,-1);
        }else if (date == DateEnum.WEEK){
            return new DateDimension(year,-1,month,week,-1);
        }else if (date == DateEnum.DAY){
            return new DateDimension(year,-1,month,-1,day);
        }
        return new DateDimension();
    }
}
