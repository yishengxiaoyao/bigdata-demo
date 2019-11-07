package com.edu.bigdata.analysis.kv.key;

import com.edu.bigdata.analysis.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 时间维度类
 */
public class DateDimension extends BaseDimension {

    // 时间维度主键
    private int id;

    // 时间维度：当前通话信息所在年
    private int year;

    // 时间维度：当前通话信息所在月，如果按照年来统计信息，则month为-1
    private int month;

    // 时间维度：当前通话信息所在日，如果按照年或者月来统计信息，则day为-1
    private int day;

    public DateDimension() {
        super();
    }

    public DateDimension(int year, int month, int day) {
        super();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DateDimension that = (DateDimension) o;

        if (year != that.year) return false;
        if (month != that.month) return false;
        return day == that.day;
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
        DateDimension anotherDateDimension = (DateDimension) o;

        int result = Integer.compare(this.id, anotherDateDimension.getId());
        if (result != 0) return result;

        result = Integer.compare(this.year, anotherDateDimension.getYear());
        if (result != 0) return result;

        result = Integer.compare(this.month, anotherDateDimension.getMonth());
        if (result != 0) return result;

        result = Integer.compare(this.day, anotherDateDimension.getDay());
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.month);
        dataOutput.writeInt(this.day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "DateDimension{" +
                "id=" + id +
                ", year=" + year +
                ", month=" + month +
                ", day=" + day +
                '}';
    }
}