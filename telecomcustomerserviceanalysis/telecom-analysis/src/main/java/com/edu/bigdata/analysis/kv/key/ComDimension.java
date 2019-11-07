package com.edu.bigdata.analysis.kv.key;

import com.edu.bigdata.analysis.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 时间维度+联系人维度的组合维度类（包装类）
 */
public class ComDimension extends BaseDimension {

    // 联系人维度
    private ContactDimension contactDimension = new ContactDimension();

    // 时间维度
    private DateDimension dateDimension = new DateDimension();

    public ComDimension() {
        super();
    }

    public ComDimension(ContactDimension contactDimension, DateDimension dateDimension) {
        super();
        this.contactDimension = contactDimension;
        this.dateDimension = dateDimension;
    }

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComDimension that = (ComDimension) o;

        if (contactDimension != null ? !contactDimension.equals(that.contactDimension) : that.contactDimension != null)
            return false;
        return dateDimension != null ? dateDimension.equals(that.dateDimension) : that.dateDimension == null;
    }

    @Override
    public int hashCode() {
        int result = contactDimension != null ? contactDimension.hashCode() : 0;
        result = 31 * result + (dateDimension != null ? dateDimension.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) return 0;
        ComDimension anotherComDimension = (ComDimension) o;

        int result = this.dateDimension.compareTo(anotherComDimension.getDateDimension());
        if (result != 0) return result;

        result = this.contactDimension.compareTo(anotherComDimension.getContactDimension());
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.contactDimension.write(dataOutput);
        this.dateDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.contactDimension.readFields(dataInput);
        this.dateDimension.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "ComDimension{" +
                "contactDimension=" + contactDimension +
                ", dateDimension=" + dateDimension +
                '}';
    }
}